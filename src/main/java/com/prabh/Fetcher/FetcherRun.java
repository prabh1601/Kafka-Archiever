package com.prabh.Fetcher;

import org.apache.kafka.clients.admin.NewTopic;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

// Source Connector
public class FetcherRun {
    public void SourceConnectorTemplate() {

        S3Client s3Client = S3Client.builder()
                .region(Region.AP_SOUTH_1)
                .build();

        FetchRequestRange start = new FetchRequestRange
                .StartTimestampBuilder(2022, 7, 3, 0, 14)
                .build();

        FetchRequestRange end = new FetchRequestRange
                .EndTimestampBuilder(2022, 7, 3, 0, 14)
                .build();


        SourceClient client = new SourceClient.Builder()
                .s3Builder(s3Client, "prabhtest", "demotest")
                .kafkaBuilder("localhost:9092", new NewTopic("demotest", 4, (short) 1))
                .range(start, end)
                .inMemoryStream()
                .build();

//      Overwrites if any previous cache available
//        client.start();

//        Resumes process using any previous cache available
        client.resume();
    }

    public static void main(String[] args) {
        new FetcherRun().SourceConnectorTemplate();
    }
}
