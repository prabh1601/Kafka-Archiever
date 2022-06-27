package com.prabh.Fetcher;

import com.prabh.Utils.Config;
import org.apache.kafka.clients.admin.NewTopic;
import software.amazon.awssdk.services.s3.S3Client;

public class FetcherRun {
    public void SourceConnectorTemplate() {
        // Source Connector
        FetchRequestRange start = new FetchRequestRange
                .StartTimestampBuilder(2022, 6, 26, 3)
                .build();

        FetchRequestRange end = new FetchRequestRange
                .EndTimestampBuilder(2022, 6, 26, 3, 33)
                .build();

        S3Client s3Client = S3Client.builder().region(Config.region).build();
        SourceApplication app = new SourceApplication.Builder()
                .bootstrapServer("localhost:9092")
                .bucket("prabhtest")
                .s3Client(s3Client)
                .consumeTopic("twitter_tweets")
                .produceTopic(new NewTopic("test", 4, (short) 1))
                .range(start, end)
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
