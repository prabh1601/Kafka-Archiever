package com.prabh.Utils;

import com.prabh.SinkArchiever.ConsumerClient;

import java.util.List;

public class Main {
    public static void main(String[] args) {

        for (int i = 0; i < 5; i++) {
            ConsumerClient consumer = new ConsumerClient.Builder()
                    .bootstrapServer("localhost:9092")
                    .consumerGroup("test_group2")
                    .autoOffsetReset("earliest")
                    .subscribe("twitter_tweets")
                    .build();
        }
    }


}
