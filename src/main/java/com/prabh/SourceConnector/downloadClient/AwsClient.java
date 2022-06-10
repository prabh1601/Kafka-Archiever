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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class AwsClient {
    private final TransferManager xfer_mgr;
    private final AmazonS3 s3Client;
    private List<MultipleFileDownload> downloads = new ArrayList<>();

    public AwsClient() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(Config.awsKeyId, Config.awsSecretKey);

        s3Client = AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTH_1)
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();

        xfer_mgr = TransferManagerBuilder.standard()
                .withS3Client(s3Client)
                .build();
    }

    public void download(String key_prefix) {
        try {
            MultipleFileDownload xfer = xfer_mgr.downloadDirectory(Config.bucket, key_prefix, new File(Config.writeDir));
            downloads.add(xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public List<MultipleFileDownload> getOngoingDownloads() {
        return this.downloads;
    }

    public void shutdown() {
        xfer_mgr.shutdownNow();
        s3Client.shutdown();
    }

}
