package com.prabh.SinkConnector;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.prabh.SinkConnector.uploadClient.AwsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;

/*
 maxConcurrentHoldings is equivalent to BlockingQueueSize
 Done to stop flooding of uploads from writingpool
 */
public class UploaderService {
    private final Logger logger = LoggerFactory.getLogger(UploaderService.class);
    private ExecutorService uploadExecutor;
    private final int noOfSimultaneousUploads;
    private final int maxConcurrentHoldings;
    final Semaphore semaphore;
    private final AwsClient s3Client = new AwsClient();

    public UploaderService(int _noOfSimultaneousUploads, int _maxConcurrentHoldings) {
        this.noOfSimultaneousUploads = _noOfSimultaneousUploads;
        this.maxConcurrentHoldings = _maxConcurrentHoldings;
        this.semaphore = new Semaphore(this.maxConcurrentHoldings);
        init();
    }

    public UploaderService(int _noOfSimultaneousUploads) {
        this(_noOfSimultaneousUploads, 3);
    }

    UploaderService() {
        this(2);
    }

    void init() {
        s3Client.initConnection(noOfSimultaneousUploads);
        // (corePoolSize, maxPoolSize, keepAliveTime, timeUnit, queueType, threadFactory, RejectedExecutionHandler)
        uploadExecutor = new ThreadPoolExecutor(this.noOfSimultaneousUploads,
                this.noOfSimultaneousUploads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(this.maxConcurrentHoldings),
                new ThreadFactoryBuilder().setNameFormat("UPLOADER-THREAD-%d").build(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public void upload(File file, String key) {
        try {
            uploadExecutor.submit(new UploadingTask(file, key));
        } catch (RejectedExecutionException e) {
            logger.error("{} Writing Task failed to stage for upload due to failure in scheduling for execution", file.getName());
            // Do something for this file here ?
        }
    }

    public void shutdown() {
        uploadExecutor.shutdown();
        try {
            uploadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        logger.warn("Uploader Client Shutdown complete");
    }

    private class UploadingTask implements Runnable {
        private final File file;
        private final String key;

        public UploadingTask(File _file, String _key) {
            this.file = _file;
            this.key = _key;
        }

        public void run() {
            semaphore.release();
            logger.info("{} Started uploading with key {}", file.getName(), key);
            long threadId = Thread.currentThread().getId() % noOfSimultaneousUploads;
            s3Client.uploadAsync((int) threadId, file, key);
            if (!file.delete()) {
                logger.error("{} Deletion failed", file.getName());
            }
        }
    }

}