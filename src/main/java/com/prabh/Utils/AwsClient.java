package com.prabh.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class AwsClient {
    private final Logger logger = LoggerFactory.getLogger(AwsClient.class);
    Properties values = new Properties();
    private S3TransferManager s3tm;

    public AwsClient() {
        try {
            values.load(new FileReader("/mnt/Drive1/JetBrains/Intellij/KafkaArchiver/src/main/Properties/values.properties"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException();
        }
//        initConnection();
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

    public void uploadObject(File file, String key) {
        Region region = Region.AP_SOUTH_1;
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(values.getProperty("bucketName"))
                .key(key)
                .build();

        s3.putObject(objectRequest, RequestBody.fromFile(file));
        logger.info("Successfully uploaded file : {}", file.getName());
    }

    public void upload(File file, String key) {
        Upload upload = s3tm.upload(b -> b.source(Paths.get(file.getAbsolutePath()))
                .putObjectRequest(req -> req.bucket(values.getProperty("bucketName"))
                        .key(key)));

        upload.completionFuture().join();
        logger.info("Successfully uploaded file : {}", file.getName());
    }

    public void uploadAsync(File file, String key) {

        Region region = Region.AP_SOUTH_1;
        S3AsyncClient client = S3AsyncClient.builder()
                .region(region)
                .build();

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(values.getProperty("bucket"))
                .key(key)
                .build();

        // Put the object into the bucket
        CompletableFuture<PutObjectResponse> future = client.putObject(objectRequest,
                AsyncRequestBody.fromFile(Paths.get(file.getAbsolutePath()))
        );
        future.whenComplete((resp, err) -> {
            try {
                if (resp == null) {
                    err.printStackTrace();
                }
            } finally {
                // Only close the client when you are completely done with it
                client.close();
            }
        });

        future.join();
    }
}
