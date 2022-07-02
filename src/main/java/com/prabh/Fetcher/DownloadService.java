package com.prabh.Fetcher;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class DownloadService {
    private final Logger logger = LoggerFactory.getLogger(DownloadService.class);
    private final int maxDepth = 5;
    private final List<Integer> maxPossible = List.of(-1, Integer.MAX_VALUE, 12, 31, 23, 59);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final FetchRequestRange start;
    private final FetchRequestRange end;
    private final String downloadTopic;
    private final S3Client s3Client;
    private final String bucket;
    private final ExecutorService workers;
    private final ProducerService producerService;
    private final boolean streamDownload;
    private final FilePaths filePaths;
    ProgressListener progressListener;

    public DownloadService(S3Client _s3Client, String _bucket, String _topic, FetchRequestRange _start,
                           FetchRequestRange _end, ProducerService _producerService, boolean _streamDownload,
                           FilePaths _filePaths, int noOfWorkerThreads) {
        this.streamDownload = _streamDownload;
        this.bucket = _bucket;
        this.start = _start;
        this.end = _end;
        this.downloadTopic = _topic;
        this.s3Client = _s3Client;
        this.producerService = _producerService;
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("DOWNLOAD-WORKER-%d").build();
        this.workers = Executors.newFixedThreadPool(noOfWorkerThreads, tf);
        this.filePaths = _filePaths;
    }

    public void shutdown(boolean forced) {
        if (forced) {
            stopped.set(true);
        }
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

    public void replayRejectedCache() {
        File rejectedCache = new File(filePaths.RejectedDirectory);
        if (rejectedCache.exists()) {
            for (File file : Objects.requireNonNull(rejectedCache.listFiles())) {
                producerService.submit(null, file.getAbsolutePath());
            }
        } else {
            logger.error("No Cached Data found for Rejected Records");
        }
        shutdown(false);
    }

    String getValidPrefix(int depth, List<Integer> state) {
        StringBuilder keyPrefixBuilder = new StringBuilder("topics/" + downloadTopic + "/");
        for (int i = 1; i <= depth; i++) {
            keyPrefixBuilder.append(state.get(i)).append("/");
        }

        return keyPrefixBuilder.toString();
    }

    long fetchObjectListWithPrefix(String bucket, String keyPrefix) {
        long totalObjects = 0;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePaths.NetObjectListFile, true))) {
            ListObjectsV2Request listObjects = ListObjectsV2Request.builder().bucket(bucket).prefix(keyPrefix).build();

            boolean done = false;
            while (!done) {
                ListObjectsV2Response listObjResponse = s3Client.listObjectsV2(listObjects);
                totalObjects += listObjResponse.contents().size();
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
            System.exit(1);
        } catch (IOException e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        return totalObjects;
    }

    long fetchList(int depth, List<Integer> state) {
        String keyPrefix = getValidPrefix(depth, state);
        return fetchObjectListWithPrefix(bucket, keyPrefix);
    }

    void stageForDownload(String bucket, String key, String localFileName) {
        File f = new File(localFileName);
        if (f.exists()) f.delete();

        workers.submit(new DownloadWorker(bucket, key, localFileName));
    }

    class DownloadWorker implements Runnable {
        String bucket;
        String key;
        String localFileName;

        public DownloadWorker(String bucket, String key, String localFileName) {
            this.bucket = bucket;
            this.key = key;
            this.localFileName = localFileName;
        }

        void downloadFile() {
            try {
                GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
                GetObjectResponse response = s3Client.getObject(request, Paths.get(localFileName));
                if (response == null) {
                    logger.error("Failed Download : {}", new File(localFileName).getName());
                    return;
                }
                progressListener.markDownloadedObject(key);
                producerService.submit(key, localFileName);
            } catch (AwsServiceException e) {
                logger.error("Download for object {} failed\n{}", key, e.awsErrorDetails().errorMessage());
            } catch (SdkClientException e) {
                logger.error(e.getMessage());
            }

        }

        void downloadStream() {
            try {
                GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();
                ResponseBytes<GetObjectResponse> response = s3Client.getObject(request, ResponseTransformer.toBytes());
                File f = new File(localFileName);
                if (response.response() == null) {
                    logger.error("Failed Download : {}", f.getName());
                    return;
                }
                progressListener.markDownloadedObject(key);
                producerService.submit(key, f.getName(), response.asByteArray());
            } catch (AwsServiceException e) {
                logger.error("Download for object {} failed\n{}", key, e.awsErrorDetails().errorMessage());
            } catch (SdkClientException e) {
                logger.error(e.getMessage());
            }

        }

        public void run() {
            if (stopped.get()) {
                return;
            }
            if (streamDownload) {
                downloadStream();
            } else {
                downloadFile();
            }
        }
    }

    long query(int currentDepth, List<Integer> currentState, boolean leftBorder, boolean rightBorder) {
        // leaf node or inside range -> query all files with same prefix
        if (currentDepth == maxDepth || (!leftBorder && !rightBorder)) {
            return fetchList(currentDepth, currentState);
        }

        int leftEndpoint = start.currentValue.get(currentDepth + 1);
        int rightEndpoint = end.currentValue.get(currentDepth + 1);
        int startingValue = leftBorder ? leftEndpoint : 0;
        int endingValue = rightBorder ? rightEndpoint : maxPossible.get(currentDepth + 1);
        int totalObjects = 0;
        for (int i = startingValue; i <= endingValue; i++) {
            currentState.add(i);
            boolean nLeftBorder = leftBorder && (i == leftEndpoint);
            boolean nRightBorder = rightBorder && (i == rightEndpoint);
            totalObjects += query(currentDepth + 1, currentState, nLeftBorder, nRightBorder);
            currentState.remove(currentState.size() - 1);
        }
        return totalObjects;
    }

    String getObjectName(String objectKey) {
        int i = objectKey.length() - 1;
        while (i >= 0 && objectKey.charAt(i) != '/') i--;
        return objectKey.substring(i + 1);
    }

    HashMap<String, Integer> previousProgressCheckpoint = new HashMap<>();

    void initiateDownloads() {
        try {
            List<String> NetObjectKeyList = Files.readAllLines(Paths.get(filePaths.NetObjectListFile));

            for (String objectKey : NetObjectKeyList) {
                String objectFilePath = filePaths.DownloadDirectory + "/" + getObjectName(objectKey);
                File f = new File(objectFilePath);
                if (!previousProgressCheckpoint.containsKey(objectKey)) {
                    stageForDownload(bucket, objectKey, objectFilePath);
                } else {
                    Integer status = previousProgressCheckpoint.get(objectKey);
                    if (status == 2) {
                        logger.info("Skipping already replayed object : {}", objectKey);
                    } else if (f.exists()) {
                        producerService.submit(objectKey, objectFilePath);
                    } else {
                        stageForDownload(bucket, objectKey, objectFilePath);
                    }
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public long loadProgressCheckpoint() {
        try {
            List<String> alreadyDownloadedObjects = Files.readAllLines(Paths.get(filePaths.DownloadedObjectListFile));
            List<String> alreadyProducedObjects = Files.readAllLines(Paths.get(filePaths.ProducedObjectListFile));
            for (String objectKey : alreadyDownloadedObjects) {
                previousProgressCheckpoint.put(objectKey, 1);
            }

            for (String objectKey : alreadyProducedObjects) {
                previousProgressCheckpoint.put(objectKey, 2);
            }

            return alreadyProducedObjects.size();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return 0;
        }
    }

    void deleteLocalCache(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteLocalCache(file);
            }
        }
        directoryToBeDeleted.delete();
    }

    void start(boolean usePreviousProgress) {
        Thread.currentThread().setName("DOWNLOAD-LEADER");

        long alreadyDone = 0;
        if (usePreviousProgress) {
            alreadyDone = loadProgressCheckpoint();
        } else {
            deleteLocalCache(new File(filePaths.localCacheDirectory));
        }

        try {
            Files.createDirectories(Paths.get(filePaths.DownloadDirectory));
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

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
        long total = query(0, currentState, true, true);
        logger.info("All valid prefixes queried");

        progressListener = new ProgressListener(filePaths);
        producerService.setProgressListener(progressListener);
        progressListener.start(5, TimeUnit.SECONDS);
        logger.info("Initiating Downloads");
        initiateDownloads();
        shutdown(false);
    }
}
