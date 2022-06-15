package com.prabh.SourceConnector.downloadClient;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.prabh.Utils.Config;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AwsClient {
    private final TransferManager xfer_mgr;
    private final AmazonS3 s3_v1;
    private final S3Client s3_v2;
    private List<MultipleFileDownload> downloads = new ArrayList<>();

    public AwsClient() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(Config.awsKeyId, Config.awsSecretKey);

        this.s3_v1 = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTH_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
        this.s3_v2 = S3Client.builder()
                .region(Config.region)
                .credentialsProvider(credentialsProvider)
                .build();

        this.xfer_mgr = TransferManagerBuilder.standard()
                .withS3Client(s3_v1)
                .build();
    }

    public void download(String key_prefix) {
        try {
            MultipleFileDownload xfer = xfer_mgr.downloadDirectory(Config.bucket, key_prefix,
                    new File(Config.DownloaderServiceDir));
            downloads.add(xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public List<MultipleFileDownload> getOngoingDownloads() {
        return this.downloads;
    }

    public void fetchObjectsWithPrefix(String keyPrefix, long startStamp) {
        try {
            ListObjectsV2Request listObjects = ListObjectsV2Request
                    .builder()
                    .bucket(Config.bucket)
                    .prefix("topics/test/")
                    .maxKeys(100)
                    .build();

            boolean done = false;
            String fileName = Config.ObjectListPath + "_" + startStamp + ".txt";
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            while (!done) {
                ListObjectsV2Response listObjResponse = s3_v2.listObjectsV2(listObjects);
                for (S3Object content : listObjResponse.contents()) {
                    writer.write(content.key() + "\n");
                }

                done = !listObjResponse.isTruncated();
                String nextToken = listObjResponse.nextContinuationToken();
                if (nextToken != null) {
                    listObjects = listObjects.toBuilder()
                            .continuationToken(listObjResponse.nextContinuationToken())
                            .build();
                }
            }
            writer.close();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        xfer_mgr.shutdownNow();
        s3_v1.shutdown();
        s3_v2.close();
    }

}
