package com.prabh.CodeExamples;

import com.prabh.Archiver.SinkApplication;
import com.prabh.Utils.CompressionType;
import TestingTools.Config;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.List;

// Template UseCases
public class ArchiverRun {
    public void SinkConnectorTemplate() {
        // SinkConnector
        FullJitterBackoffStrategy backoffStrategy = FullJitterBackoffStrategy.builder()
                .baseDelay(Duration.ofMillis(200))
                .maxBackoffTime(Duration.ofHours(24))
                .build();

        RetryPolicy policy = RetryPolicy.builder()
                .numRetries(3)
                .additionalRetryConditionsAllowed(true)
                .backoffStrategy(backoffStrategy)
                .throttlingBackoffStrategy(backoffStrategy)
                .build();

        ClientOverrideConfiguration conf = ClientOverrideConfiguration.builder().retryPolicy(policy).build();

        // aws client
        S3Client s3Client = S3Client.builder().region(Region.AP_SOUTH_1)
                .overrideConfiguration(conf)
                .build();

        SinkApplication app = new SinkApplication.Builder()
                .bootstrapServer("localhost:9092")
                .subscribedTopics(List.of("twitter_tweets"))
                .s3(s3Client, "prabhtest")
                .compressionType(CompressionType.GZIP)
                .build();

        app.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("Shutdown Hook");
            app.shutdown();
        }));
    }

    public static void main(String[] args) throws InterruptedException {
        new ArchiverRun().SinkConnectorTemplate();
    }
}
