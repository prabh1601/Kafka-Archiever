package com.prabh.CodeExamples;

import TestingTools.Config;
import com.prabh.Fetcher.FetchRequestRange;
import com.prabh.Fetcher.SourceApplication;
import org.apache.kafka.clients.admin.NewTopic;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

public class FetcherRun {
    public void SourceConnectorTemplate() {
        // Source Connector
//        FetchRequestRange start = new FetchRequestRange
//                .StartTimestampBuilder(2022, 6, 25, 20, 21)
//                .build();
//
//        FetchRequestRange end = new FetchRequestRange
//                .EndTimestampBuilder(2022, 6, 25, 20, 21)
//                .build();

        FetchRequestRange start = new FetchRequestRange
                .StartTimestampBuilder(2022, 6, 28, 1, 10)
                .build();

        FetchRequestRange end = new FetchRequestRange
                .EndTimestampBuilder(2022, 6, 28, 1, 10)
                .build();


        FullJitterBackoffStrategy backoffStrategy = FullJitterBackoffStrategy.builder().baseDelay(Duration.ofMillis(200))
                .maxBackoffTime(Duration.ofHours(24)).build();

        RetryPolicy policy = RetryPolicy.builder().numRetries(3).additionalRetryConditionsAllowed(true)
                .backoffStrategy(backoffStrategy).throttlingBackoffStrategy(backoffStrategy).build();

        ClientOverrideConfiguration conf = ClientOverrideConfiguration.builder().retryPolicy(policy).build();

        S3Client s3Client = S3Client.builder().region(Region.AP_SOUTH_1).overrideConfiguration(conf).build();

        SourceApplication app = new SourceApplication.Builder()
                .s3Builder(s3Client, "prabhtest", "test")
                .kafkaBuilder("localhost:9092", new NewTopic("test", 4, (short) 1))
                .range(start, end)
                .build();

        // Overwrites if any previous cache available
        app.start();

        // Resumes process using any previous cache available
//        app.resume();

        // Replays the rejected cache from any previous runs if present
//        app.replayRejectedCache();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("Shutdown Hook");
            app.shutdown();
        }));
    }

    public static void main(String[] args) {
        new FetcherRun().SourceConnectorTemplate();
    }
}
