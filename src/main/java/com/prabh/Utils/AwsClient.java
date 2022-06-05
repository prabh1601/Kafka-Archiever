package com.prabh.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.transfer.s3.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class AwsClient {
    private final Logger logger = LoggerFactory.getLogger(AwsClient.class.getName());
    Properties values = new Properties();
    private S3TransferManager s3tm;

    public AwsClient() {
        try {
            values.load(new FileReader("/mnt/Drive1/JetBrains/Intellij/KafkaArchiver/src/main/java/com/prabh/values.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        initConnection();
    }

    public void initConnection() {
        Region region = Region.AP_SOUTH_1;
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(values.getProperty("awsKeyId"), values.getProperty("awsSecretKey"));
        S3ClientConfiguration s3ClientConfiguration = S3ClientConfiguration.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .minimumPartSizeInBytes((long) (9 * MB))
                .targetThroughputInGbps(20.0)
                .build();

        s3tm = S3TransferManager.builder()
                .s3ClientConfiguration(s3ClientConfiguration)
                .build();
    }

    public void upload(File file, String key) {
        Upload upload = s3tm.upload(b -> b.source(Paths.get(file.getAbsolutePath()))
                .putObjectRequest(req -> req.bucket(values.getProperty("bucketName"))
                        .key(key)));

        upload.completionFuture().join();
        logger.info("Successfully uploaded file : {}", file.getName());
    }
}
