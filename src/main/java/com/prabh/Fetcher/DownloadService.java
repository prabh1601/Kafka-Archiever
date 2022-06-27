package com.prabh.Fetcher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DownloadService {
    private final Logger logger = LoggerFactory.getLogger(DownloadService.class);
    private final int maxDepth = 5;
    private final List<Integer> maxPossible = List.of(-1, Integer.MAX_VALUE, 12, 31, 23, 59);
    private final FetchRequestRange start;
    private final FetchRequestRange end;
    private final String downloadTopic;
    private final S3Client s3Client;
    private final String localDumpLocation;
    private final String objectListFilePath;
    private final String downloadLocation;
    private final String bucket;
    private final ExecutorService workers;
    private final ProducerService producerService;
    private final boolean streamDownload;

    public DownloadService(S3Client _s3Client, String _bucket, String _topic, FetchRequestRange _start,
                           FetchRequestRange _end, ProducerService _producerService, boolean _streamDownload,
                           String _localDumpLocation, int noOfWorkerThreads) {
        this.streamDownload = _streamDownload;
        this.bucket = _bucket;
        this.start = _start;
        this.end = _end;
        this.downloadTopic = _topic;
        this.s3Client = _s3Client;
        this.producerService = _producerService;
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("DOWNLOAD-WORKER-%d").build();
        this.workers = Executors.newFixedThreadPool(noOfWorkerThreads, tf);
        this.localDumpLocation = _localDumpLocation;
        this.objectListFilePath = localDumpLocation + "/ObjectKeys";
        this.downloadLocation = localDumpLocation + "/DownloadDump";
    }

    public void shutdown() {
        workers.shutdown();
        try {
            workers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            s3Client.close();
            logger.info("Downloads Completed");
            logger.warn("Download Service Shutting down");
            producerService.shutdown();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            Thread.currentThread().interrupt();  // set interrupt flag
        }
    }

    public String getValidPrefix(int depth, List<Integer> state) {
        StringBuilder keyPrefixBuilder = new StringBuilder("topics/" + downloadTopic + "/");
        for (int i = 1; i <= depth; i++) {
            keyPrefixBuilder.append(state.get(i)).append("/");
        }

        return keyPrefixBuilder.toString();
    }

    public void fetchObjectListWithPrefix(String bucket, String keyPrefix) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(objectListFilePath, true))) {
            ListObjectsV2Request listObjects = ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix).build();

            boolean done = false;
            while (!done) {
                ListObjectsV2Response listObjResponse = s3Client.listObjectsV2(listObjects);
                for (S3Object content : listObjResponse.contents()) {
                    writer.write(content.key() + "\n");
                }

                done = !listObjResponse.isTruncated();
                String nextToken = listObjResponse.nextContinuationToken();
                if (nextToken != null) {
                    listObjects = listObjects.toBuilder().continuationToken(listObjResponse.nextContinuationToken()).build();
                }
            }
        } catch (S3Exception e) {
            logger.error(e.awsErrorDetails().errorMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void fetchList(int depth, List<Integer> state) {
        String keyPrefix = getValidPrefix(depth, state);
        fetchObjectListWithPrefix(bucket, keyPrefix);
    }

    public void stageForDownload(String bucket, String key, String localFileName) {
        workers.submit(new DownloadWorker(bucket, key, localFileName));
    }

    public class DownloadWorker implements Runnable {
        String bucket;
        String key;
        String localFileName;

        public DownloadWorker(String bucket, String key, String localFileName) {
            this.bucket = bucket;
            this.key = key;
            this.localFileName = localFileName;
        }

        public void downloadFile() {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
            GetObjectResponse response = s3Client.getObject(request, Paths.get(localFileName));
            if (response == null) {
                logger.error("Failed Download : {}", new File(localFileName).getName());
                return;
            }
            producerService.submit(localFileName);
        }

        public void downloadStream() {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
            ResponseBytes<GetObjectResponse> response = s3Client.getObject(request, ResponseTransformer.toBytes());
            File f = new File(localFileName);
            if (response.response() == null) {
                logger.error("Failed Download : {}", f.getName());
                return;
            }
            producerService.submit(f.getName(), response.asByteArray());
        }

        public void run() {
            if (streamDownload) {
                downloadStream();
            } else {
                downloadFile();
            }
        }
    }

    public void query(int currentDepth, List<Integer> currentState, boolean leftBorder, boolean rightBorder) {
        // leaf node or inside range -> query all files with same prefix
        if (currentDepth == maxDepth || (!leftBorder && !rightBorder)) {
            fetchList(currentDepth, currentState);
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
        }
    }

    public String getBatchName(String batchKey) {
        int i = batchKey.length() - 1;
        while (i >= 0 && batchKey.charAt(i) != '/') i--;
        return batchKey.substring(i + 1);
    }

    public void initiateDownloads() {
        try {
            List<String> archivedBatchesKeyList = Files.readAllLines(Paths.get(objectListFilePath), StandardCharsets.UTF_8);
            producerService.setBatchCount(archivedBatchesKeyList.size());
            for (String batchKey : archivedBatchesKeyList) {
                String batchFilePath = downloadLocation + "/" + getBatchName(batchKey);
                System.out.println(batchFilePath);
                stageForDownload(bucket, batchKey, batchFilePath);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public void run() {
        Thread.currentThread().setName("DOWNLOAD-LEADER");
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
        try {
            Files.createDirectories(Paths.get(localDumpLocation));
            Files.createDirectories(Paths.get(downloadLocation));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        logger.info("Generating Valid prefixes");
        query(0, currentState, true, true);
        logger.info("All valid prefixes queried");
        logger.info("Object List Fetching Complete. Local Cache List can be found at : {}", objectListFilePath);
        logger.info("Initiating Downloads");
        initiateDownloads();
        shutdown();
    }
}
