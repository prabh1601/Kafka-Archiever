package com.prabh.Utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.transfer.s3.CompletedUpload;
import software.amazon.awssdk.transfer.s3.S3ClientConfiguration;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.Upload;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.amazonaws.services.s3.internal.Constants.MB;

public class AwsClient {
    private final Logger logger = LoggerFactory.getLogger(AwsClient.class.getName());
    Properties values = new Properties();

    AwsClient() throws IOException {
        values.load(new FileReader("/mnt/Drive1/JetBrains/Intellij/KafkaArchiver/src/main/java/com/prabh/values.properties"));
    }

    public void synchronousClient(File f) {
        AWSCredentials credentials = new BasicAWSCredentials(values.getProperty("awsKeyId"),values.getProperty("awsSecretKey"));
        AmazonS3 s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_2)
                .build();
        s3client.putObject("potatodac", "a/f/s", f);
    }

    public class asynchronousClient implements Runnable {
        public void run() {
            String path = "/mnt/Drive1/Test/test2.txt";
            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(values.getProperty("awxsKeyId"),values.getProperty("awsSecretKey"));
            S3AsyncClient s3client = S3AsyncClient.builder()
                    .region(Region.US_EAST_2)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .build();


            PutObjectRequest objectRequest = PutObjectRequest.builder()
                    .bucket(values.getProperty("bucketName"))
                    .key("asynctest")
                    .build();

            System.out.println();
            CompletableFuture<PutObjectResponse> future = s3client.putObject(objectRequest, AsyncRequestBody.fromFile(Paths.get(path)));
            future.whenComplete((resp, err) -> {
                System.out.println("executed");
                try {
                    if (resp != null) {
                        System.out.println("Object uploaded. Details: " + resp);
                    } else {
                        // Handle error.
                        System.out.println("Something went wrong");
                        err.printStackTrace();
                    }
                } finally {
                    // Only close the client when you are completely done with it.
                    s3client.close();
                }
            });

            for (int i = 0; i < 5; i++) {
                System.out.println(Thread.currentThread().getName() + " Test");
            }
            future.join();
        }
    }

    public void transferManager() throws IOException {
        String path = "/mnt/Drive1/Test/test2.txt";
        Region region = Region.US_EAST_2;

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(values.getProperty("awsKeyId"),values.getProperty("awsSecretKey"));
        S3ClientConfiguration s3ClientConfiguration = S3ClientConfiguration.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .minimumPartSizeInBytes((long) (10 * MB))
                .targetThroughputInGbps(20.0)
                .build();

        S3TransferManager s3tm = S3TransferManager.builder()
                .s3ClientConfiguration(s3ClientConfiguration)
                .build();
        Upload upload = s3tm.upload(b -> b.putObjectRequest(r -> r.bucket(values.getProperty("bucketName")).key("test/test3"))
                .source(Paths.get(path)));
        for (int i = 0; i < 5; i++) {
            System.out.println("Test");
        }
//        upload.completionFuture().thenRun(() -> System.out.println("Done"));
//        CompletedUpload completedUpload = upload.completionFuture().join();
//        System.out.println("PutObjectResponse: " + completedUpload.response());
    }

    public void run() throws Exception {
        ExecutorService test = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; i++) {
            test.submit(new asynchronousClient());
        }
    }

    public static void main(String[] args) throws Exception {
        new AwsClient().run();
    }
}
