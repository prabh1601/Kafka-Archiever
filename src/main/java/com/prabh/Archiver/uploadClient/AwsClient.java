package com.prabh.SinkConnector.uploadClient;

import com.prabh.Utils.Config;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static software.amazon.awssdk.transfer.s3.SizeConstant.MB;

public class AwsClient {
    private final Logger logger = LoggerFactory.getLogger(AwsClient.class);
    private List<S3TransferManager> tmClients;
    private List<S3AsyncClient> asyncClients;
    private List<S3Client> syncClients;
    private final String bucket = Config.bucket;
    private final Region region = Config.region;
    private final int noOfClients;

    public AwsClient(int _noOfClients) {
        noOfClients = _noOfClients;
//        initAsyncConnection();
        initSyncConnection();
//        initTmConnection();
    }

    // ASYNC CLIENT
    private void initAsyncConnection() {
        asyncClients = new ArrayList<>(noOfClients);
        for (int i = 0; i < noOfClients; i++) {
            asyncClients.add(getAsyncClient());
        }
    }

    private S3AsyncClient getAsyncClient() {
        return S3AsyncClient.builder()
                .region(region)
                .build();
    }

    public void uploadAsync(int clientNo, File file, String key) {
        S3AsyncClient client = asyncClients.get(clientNo);
        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        // Put the object into the bucket
        CompletableFuture<PutObjectResponse> future = client.putObject(objectRequest,
                AsyncRequestBody.fromFile(Paths.get(file.getAbsolutePath()))
        );

        future.whenComplete((resp, err) -> {
            if (resp == null) {
                err.printStackTrace();
            }
        });

        future.join();
    }

    // SYNC CLIENT
    private void initSyncConnection() {
        syncClients = new ArrayList<>(noOfClients);
        for (int i = 0; i < noOfClients; i++) {
            syncClients.add(getSyncClient());
        }
    }

    private S3Client getSyncClient() {
        return S3Client.builder()
                .region(region)
                .build();
    }

    public void uploadSync(int clientNo, File file, String key) {

        S3Client client = syncClients.get(clientNo);

        PutObjectRequest objectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        client.putObject(objectRequest, RequestBody.fromFile(file));
        logger.info("Successfully uploaded file : {}", file.getName());
    }

    // TRANSFER MANAGER
    private void initTmConnection() {
        tmClients = new ArrayList<>(noOfClients);
        for (int i = 0; i < noOfClients; i++) {
            tmClients.add(getTmClient());
        }
    }

    private S3TransferManager getTmClient() {

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(Config.awsKeyId, Config.awsSecretKey);
        S3ClientConfiguration s3ClientConfiguration = S3ClientConfiguration.builder()
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .region(region)
                .minimumPartSizeInBytes((long) (5 * MB))
                .targetThroughputInGbps(20.0)
                .build();

        return S3TransferManager.builder()
                .s3ClientConfiguration(s3ClientConfiguration)
                .build();
    }

    public void uploadTm(int clientNo, File file, String key) {
        S3TransferManager s3tm = tmClients.get(clientNo);

        Upload upload = s3tm.upload(b -> b.source(Paths.get(file.getAbsolutePath()))
                .putObjectRequest(req -> req.bucket(bucket)
                        .key(key)));

        upload.completionFuture().join();
        logger.info("Successfully uploaded file : {}", file.getName());
    }
}
