package com.prabh.Utils;

import com.prabh.SinkArchiever.SinkApplication;

// Template UseCase of SinkApplication
public class Main {
    public static void main(String[] args) throws InterruptedException {
        SinkApplication app = new SinkApplication.Builder()
                .bootstrapServer("localhost:9092")
                .consumerGroup("cg")
                .consumerCount(3)
                .subscribedTopic("test")
                .writeTaskCount(10)
                .build();

        app.start();

    }
}
