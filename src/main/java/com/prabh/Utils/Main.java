package com.prabh.Utils;

import com.prabh.SinkArchiever.ConsumerThread;

public class Main {
    public static void main(String[] args) {
        ConsumerThread c = new ConsumerThread.Builder()
                .bootstrapServer("localhost:9092")
                .consumerGroup("dummy-group")
                .threadCount(3)
                .autoOffsetReset("earliest")
                .subscribe("twitter_tweets")
                .build();



    }


}
