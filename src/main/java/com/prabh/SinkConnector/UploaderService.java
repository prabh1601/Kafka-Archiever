package com.prabh.SinkConnector;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class UploaderClient {
    private final Logger logger = LoggerFactory.getLogger(UploaderClient.class);
    private ExecutorService uploadExecutor;
    private final int noOfSimultaneousUploads;
    // maxConcurrentHoldings is equivalent to = UploadPoolSize + BlockingQueueSize
    // Done to stop flooding of uploads from writingpool
    private final int maxConcurrentHoldings;
    private final Semaphore semaphore;
    private final AwsClient s3Client = new AwsClient();
    private final AtomicLong curCount = new AtomicLong();
    private final AtomicLong peakCount = new AtomicLong();

    public UploaderClient(int _noOfSimultaneousUploads, int _maxConcurrentHoldings) {
        this.noOfSimultaneousUploads = _noOfSimultaneousUploads;
        this.maxConcurrentHoldings = _maxConcurrentHoldings;
        this.semaphore = new Semaphore(this.maxConcurrentHoldings);
        init();
    }

    public UploaderClient(int _noOfSimultaneousUploads) {
        this(_noOfSimultaneousUploads, 5);
    }

    UploaderClient() {
        this(3);
    }

    void init() {
        s3Client.initConnection(noOfSimultaneousUploads);

        // Constructor = (corePoolSize, maxPoolSize, keepAliveTime, timeUnit, queueType, threadFactory, RejectedExecutionPolicy)
        uploadExecutor = new ThreadPoolExecutor(this.noOfSimultaneousUploads,
                this.noOfSimultaneousUploads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(this.maxConcurrentHoldings - this.noOfSimultaneousUploads),
                new ThreadFactoryBuilder().setNameFormat("UPLOADER-THREAD-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy());

    }

    public void upload(File file, String key) {
        curCount.incrementAndGet();
        try {
            uploadExecutor.submit(new UploadingTask(file, key));
        } catch(RejectedExecutionException e){
            logger.error("{} Writing Task failed to stage for upload due to failure in scheduling for execution", file.getName());
            // Do something for this file here ?
        }
        peakCount.set(Math.max(peakCount.get(), curCount.get()));
    }

    public void shutdown() {
        uploadExecutor.shutdown();
        try {
            uploadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        logger.warn("{} max tasks at a go", peakCount.get());
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
            logger.info("{} Started uploading with key {}", file.getName(), key);
            long threadId = Thread.currentThread().getId() % noOfSimultaneousUploads;
            logger.error("{}",threadId);
            s3Client.uploadAsync((int) threadId, file, key);
            if (!file.delete()) {
                logger.error("{} Deletion failed", file.getName());
            }
            curCount.decrementAndGet();
        }
    }

}