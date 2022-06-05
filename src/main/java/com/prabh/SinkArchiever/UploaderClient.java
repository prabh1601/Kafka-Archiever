package com.prabh.SinkArchiever;

import com.prabh.Utils.AwsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UploaderClient {
    private final Logger logger = LoggerFactory.getLogger(UploaderClient.class.getName());
    private final ExecutorService uploadExecutor;
    private final int noOfSimultaneousuploads;
    private final AwsClient s3Client = new AwsClient();

    public UploaderClient() {
        this.noOfSimultaneousuploads = 3;
        uploadExecutor = Executors.newFixedThreadPool(noOfSimultaneousuploads);
    }

    public void upload(File file, String key) {
        uploadExecutor.submit(new UploadingTask(file, key));
    }

    private class UploadingTask implements Runnable {
        private final File file;
        private final String key;

        public UploadingTask(File _file, String _key) {
            this.file = _file;
            this.key = _key;
        }

        public void run() {
            s3Client.upload(file, key);
        }
    }

}