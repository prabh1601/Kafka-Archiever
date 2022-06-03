package com.prabh.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.transfer.s3.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class AwsClient {
    private final Logger logger = LoggerFactory.getLogger(AwsClient.class.getName());
    Properties values = new Properties();

    public AwsClient() {
        try {
            values.load(new FileReader("/mnt/Drive1/JetBrains/Intellij/KafkaArchiver/src/main/java/com/prabh/values.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void uploadBatch(String dirPath) {
        Region region = Region.US_EAST_2;
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(values.getProperty("awsKeyId"), values.getProperty("awsSecretKey"));
        S3ClientConfiguration s3ClientConfiguration = S3ClientConfiguration.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .minimumPartSizeInBytes((long) (10 * MB))
                .targetThroughputInGbps(20.0)
                .build();

        S3TransferManager s3tm = S3TransferManager.builder()
                .s3ClientConfiguration(s3ClientConfiguration)
                .build();
        File dir = new File(dirPath);
        File[] files = dir.listFiles();
        List<Upload> scheduledUploads = new ArrayList<>();
        for (File file : files) {
            Upload upload = s3tm.upload(b -> b.source(Paths.get(file.getAbsolutePath()))
                    .putObjectRequest(req -> req.bucket(values.getProperty("bucketName"))
                            .key(file.getName())));
            scheduledUploads.add(upload);

        }

        for (Upload upload : scheduledUploads) {
            upload.completionFuture().join();
            logger.info("Successfully uploaded file");
        }
        logger.info("Batch Upload Complete");
    }
}
