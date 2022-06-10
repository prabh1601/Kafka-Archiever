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

public class DownloadingService {
    private final Logger logger = LoggerFactory.getLogger(DownloadingService.class);
    private final RequestObject start;
    private final RequestObject end;
    private final int maxDepth = 5;
    private final List<Integer> maxPossible;
    private final List<String> levelPrefix;
    private final String downloadTopic;
    private final AwsClient awsClient = new AwsClient();

    public DownloadingService(String _topic, RequestObject _start, RequestObject _end) {
        this.start = _start;
        this.end = _end;
        this.maxPossible = List.of(-1, Integer.MAX_VALUE, 12, 31, 23, 59);
        this.levelPrefix = List.of("topic", "year=", "month=", "day=", "hour=", "");
        this.downloadTopic = _topic;
    }

    public void stageForDownload(int depth, List<Integer> state) {

        StringBuilder keyPrefixBuilder = new StringBuilder("topics/" + downloadTopic + "/");
        for (int i = 1; i <= depth; i++) {
            keyPrefixBuilder.append(levelPrefix.get(i)).append(state.get(i));
            if (i != maxDepth) keyPrefixBuilder.append("/");
        }

        String keyPrefix = keyPrefixBuilder.toString();

        logger.info("Fetching files with prefix {}", keyPrefix);
        awsClient.download(keyPrefix);
    }

    public void query(int currentDepth, List<Integer> currentState, boolean leftBorder, boolean rightBorder) {
        // leaf node or inside range -> download all files with same prefix
        if (currentDepth == maxDepth || (!leftBorder && !rightBorder)) {
            stageForDownload(currentDepth, currentState);
            return;
        }

        int leftEndpoint = start.currentValue.get(currentDepth);
        int rightEndpoint = end.currentValue.get(currentDepth);
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
        List<Integer> currentState = new ArrayList<>(maxDepth + 1);
        currentState.add(-1);
        query(0, currentState, true, true);
        waitForCompletion();
    }

    public void waitForCompletion() {
        List<MultipleFileDownload> downloads = awsClient.getOngoingDownloads();
        for (MultipleFileDownload xfer : downloads) {
//            XferMgrProgress.showTransferProgress(xfer);
            XferMgrProgress.waitForCompletion(xfer);
            String keyPrefix = xfer.getKeyPrefix();
            createKafkaTask(keyPrefix);
        }

        awsClient.shutdown();
    }

    public void createKafkaTask(String keyPrefix) {
        StringBuilder pathName = new StringBuilder(Config.writeDir + keyPrefix);
        while (pathName.charAt(pathName.length() - 1) != '/') {
            pathName.setLength(pathName.length() - 1);
        }
        String filePath = pathName.toString();
        File dir = new File(filePath);
        if (dir.exists()) {
            fetchLocalFiles(dir);
        }
    }

    /*
    Pending :
    Check if delete is thread safe
    It might happen that transfer manager is writing into the same directory and the same time delete is triggered
     */
    public void fetchLocalFiles(File dir) {
        File[] files = dir.listFiles();
        if (files == null) {
            System.out.println(dir.getAbsolutePath());
            dir.delete();
            return;
        }
        for (File file : files) {
            fetchLocalFiles(file);
        }
        dir.delete();
    }
}
