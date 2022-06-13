package com.prabh.SourceConnector;

import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.prabh.SourceConnector.downloadClient.AwsClient;
import com.prabh.SourceConnector.downloadClient.XferMgrProgress;
import com.prabh.Utils.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DownloadingService implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(DownloadingService.class);
    private final RequestObject start;
    private final RequestObject end;
    private final int maxDepth = 5;
    private final List<Integer> maxPossible;
    private final List<String> levelPrefix;
    private final String downloadTopic;
    private final AwsClient awsClient = new AwsClient();
    private final ProducerService producerService;
    // Intended : Use Blocking Queue for Fetched files

    public DownloadingService(String _topic, RequestObject _start, RequestObject _end, ProducerService _producerService) {
        this.start = _start;
        this.end = _end;
        this.maxPossible = List.of(-1, Integer.MAX_VALUE, 12, 31, 23, 59);
        this.levelPrefix = List.of("topic", "year=", "month=", "day=", "hour=", "");
        this.downloadTopic = _topic;
        this.producerService = _producerService;
    }

    public void shutdown() {
        awsClient.shutdown();
        try {
            producerService.shutdown();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String getValidPrefix(int depth, List<Integer> state) {

        StringBuilder keyPrefixBuilder = new StringBuilder("topics/" + downloadTopic + "/");
        for (int i = 1; i <= depth; i++) {
            keyPrefixBuilder.append(levelPrefix.get(i)).append(state.get(i));
            if (i != maxDepth) keyPrefixBuilder.append("/");
        }

        return keyPrefixBuilder.toString();
    }

    public void stageForDownload(int depth, List<Integer> state) {
        String keyPrefix = getValidPrefix(depth, state);
//        logger.info("Valid Prefix hit : {}", keyPrefix);
        awsClient.download(keyPrefix);
    }

    public void query(int currentDepth, List<Integer> currentState, boolean leftBorder, boolean rightBorder) {
        // leaf node or inside range -> download all files with same prefix
        if (currentDepth == maxDepth || (!leftBorder && !rightBorder)) {
            stageForDownload(currentDepth, currentState);
            return;
        }

        int leftEndpoint = start.currentValue.get(currentDepth + 1);
        int rightEndpoint = end.currentValue.get(currentDepth + 1);
        int startingValue = leftBorder ? leftEndpoint : 0;
        int endingValue = rightBorder ? rightEndpoint : maxPossible.get(currentDepth + 1);
        for (int i = startingValue; i <= endingValue; i++) {
            currentState.add(i);
            boolean nLeftBorder = leftBorder && (i == leftEndpoint);
            boolean nRightBorder = rightBorder && (i == rightEndpoint);
            query(currentDepth + 1, currentState, nLeftBorder, nRightBorder);
            currentState.remove(currentState.size() - 1);
            // Why is there no c++ like pop_back() :/
        }
    }

    public void run() {
        Thread.currentThread().setName("DOWNLOADER-THREAD");
        logger.info("""
                        Data Fetching Started
                        Query Range :
                              Start prefix : {}
                              End prefix   : {}
                        """,
                getValidPrefix(5, start.currentValue),
                getValidPrefix(5, end.currentValue));
        List<Integer> currentState = new ArrayList<>(maxDepth + 1);
        currentState.add(-1);
        logger.info("Generating Valid prefixes");
        query(0, currentState, true, true);
        logger.info("All valid prefixes queried");
        waitForCompletion();
        logger.info("Data Fetching Complete");
        shutdown();
    }

    public void waitForCompletion() {
        List<MultipleFileDownload> downloads = awsClient.getOngoingDownloads();
        for (MultipleFileDownload xfer : downloads) {
            String keyPrefix = xfer.getKeyPrefix();
//            XferMgrProgress.showTransferProgress(xfer);
            XferMgrProgress.waitForCompletion(xfer);
            createKafkaTask(keyPrefix);
        }
    }

    public void createKafkaTask(String keyPrefix) {
        String completePrefix = Config.writeDir + keyPrefix;
        StringBuilder pathName = new StringBuilder(completePrefix);
        while (pathName.charAt(pathName.length() - 1) != '/') {
            pathName.setLength(pathName.length() - 1);
        }
        String filePath = pathName.toString();
        File dir = new File(filePath);
        if (dir.exists()) {
            int fetched = fetchLocalFiles(dir, completePrefix);
            if (fetched != 0) {
                logger.info("{} files downloaded matching prefix {}", fetched, keyPrefix);
            }
        }
    }

    /*
    Pending :
    Check if delete is thread safe
    It might happen that transfer manager is writing into the same directory and the same time delete is triggered
     */
    public int fetchLocalFiles(File dir, String prefix) {
        File[] files = dir.listFiles();
        if (files == null) {
            String filePath = dir.getAbsolutePath();
            if (filePath.startsWith(prefix)) {
                producerService.submit(dir.getAbsolutePath());
                return 1;
            }
            return 0;
        }
        int fetched = 0;
        for (File file : files) {
            fetched += fetchLocalFiles(file, prefix);
        }
        dir.delete();
        return fetched;
    }
}
