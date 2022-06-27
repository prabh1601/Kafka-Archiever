package com.prabh.Archiver;

import java.util.List;

// Template UseCases
public class ArchiverRun {
    public void SinkConnectorTemplate() {
        // SinkConnector
        SinkApplication app = new SinkApplication.Builder()
                .bootstrapServer("localhost:9092")
                .consumerGroup("archiver")
                .subscribedTopics(List.of("twitter_tweets"))
                .consumerCount(5)
                .writeTaskCount(5)
                .sinkBucket("prabhtest")
                .build();

        // aws client
        // defaults values for consumer count unless
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
