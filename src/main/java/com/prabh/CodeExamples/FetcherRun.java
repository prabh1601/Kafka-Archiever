package com.prabh.CodeExamples;

import com.prabh.Fetcher.FetchRequestRange;
import com.prabh.Fetcher.SourceApplication;
import com.prabh.Utils.Config;
import org.apache.kafka.clients.admin.NewTopic;
import software.amazon.awssdk.core.ServiceConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FetcherRun {
    public void SourceConnectorTemplate() {
        // Source Connector
        FetchRequestRange start = new FetchRequestRange
                .StartTimestampBuilder(2022, 6, 26, 16, 37)
                .build();

        FetchRequestRange end = new FetchRequestRange
                .EndTimestampBuilder(2022, 6, 26, 16, 37)
                .build();

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

        S3Client s3Client = S3Client.builder().region(Config.region)
                .overrideConfiguration(conf)
                .build();

        SourceApplication app = new SourceApplication.Builder()
                .s3(s3Client, "prabhtssdfsdest", "twitter_tweets")
                .range(start, end)
                .bootstrapServer("localhost:9092")
                .produceTopic(new NewTopic("test", 4, (short) 1))
                .build();

        app.start();

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
