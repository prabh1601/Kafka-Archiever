package com.prabh.Archiver;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.prabh.Utils.Config;
import com.prabh.Utils.LimitedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.util.concurrent.*;

public class UploadService {
    private final Logger logger = LoggerFactory.getLogger(UploadService.class);
    private final S3Client s3Client;
    private final ExecutorService uploadWorker;
    private final String bucket;

    public UploadService(String _bucket, int uploadPoolSize) {
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("UPLOAD-WORKER-%d").build();
        this.uploadWorker = new ThreadPoolExecutor(uploadPoolSize,
                uploadPoolSize,
                0L, TimeUnit.SECONDS,
                new LimitedQueue<>(2),
                namedThreadFactory);

        this.s3Client = S3Client.builder()
                .region(Config.region)
                .build();

        this.bucket = _bucket;
    }

    public UploadService(String _bucket) {
        this(_bucket, 5);
    }

    public void submit(File file, String key) {
        uploadWorker.submit(new UploadWorker(file, key));
    }

    public void shutdown() {
        uploadWorker.shutdown();
        try {
            uploadWorker.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public class UploadWorker implements Runnable {
        private final String key;
        private final File file;

        public UploadWorker(File file, String _key) {
            this.file = file;
            this.key = _key;
        }

        public void run() {
            try {
                PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();
                PutObjectResponse response = s3Client.putObject(request, RequestBody.fromFile(file));
                if (response != null) {
                    logger.info("Submitted {}", key);
                }
                if (!file.delete()) {
                    logger.error("Failed Local Cache deletion of {}", file.getName());
                }
            } catch (AwsServiceException | SdkClientException e) {
                logger.error(e.getMessage());
            }
        }
    }
}