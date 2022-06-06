package com.prabh.SinkArchiever;

import com.prabh.Utils.AwsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UploaderClient {
    private final Logger logger = LoggerFactory.getLogger(UploaderClient.class);
    private final ExecutorService uploadExecutor;
    private final int noOfSimultaneousuploads;
    private final AwsClient s3Client = new AwsClient();

    UploaderClient() {
        this.noOfSimultaneousuploads = 3;
        uploadExecutor = Executors.newFixedThreadPool(noOfSimultaneousuploads);
    }

    public UploaderClient(int _noOfSimultaneousUploads) {
        this.noOfSimultaneousuploads = _noOfSimultaneousUploads;
        uploadExecutor = Executors.newFixedThreadPool(noOfSimultaneousuploads);
    }

    public void upload(File file, String key) {
        uploadExecutor.submit(new UploadingTask(file, key));
    }

    public void shutdown() {
        uploadExecutor.shutdown();
        try {
            uploadExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch(InterruptedException e){
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
            logger.info("{} Started uploading with key {}", file.getName(), key);
            s3Client.uploadObject(file, key);
            if (!file.delete()) {
                logger.error("{} Deletion failed", file.getName());
            }
        }
    }

}