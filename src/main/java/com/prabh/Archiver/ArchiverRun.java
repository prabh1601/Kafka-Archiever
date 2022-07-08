package com.prabh.Archiver;

import com.prabh.Utils.CompressionType;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

// SinkConnector
public class ArchiverRun {
    public void SinkConnectorTemplate() {
        // aws client
        S3Client s3Client = S3Client.builder()
                .region(Region.AP_SOUTH_1)
                .build();

        SinkClient client = new SinkClient.Builder()
                .bootstrapServer("localhost:9092")
                .subscribedTopics("demoTopic100")
                .s3Builder(s3Client, "final-project-demo")
                .consumerGroup("dt4")
                .compressionType(CompressionType.GZIP)
                .build();

        client.start();
    }

    public static void main(String[] args) throws InterruptedException {
        new ArchiverRun().SinkConnectorTemplate();
    }
}
