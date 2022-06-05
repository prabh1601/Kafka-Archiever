package com.prabh.Utils;

import com.prabh.SinkArchiever.ConsumerClient;

// Template UseCase of Consumer Client
public class Main {
    public static void main(String[] args) throws InterruptedException {
        ConsumerClient c = new ConsumerClient.Builder()
                .bootstrapServer("localhost:9092")
                .consumerGroup("cg")
                .consumerCount(3)
                .taskCount(12)
                .subscribedTopic("test")
                .build();

        c.start();


    }
}
