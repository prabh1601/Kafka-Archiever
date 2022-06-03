package com.prabh.Utils;

import com.prabh.SinkArchiever.Collector;
import com.prabh.SinkArchiever.ConsumerClient;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// Template UseCase of Consumer Client
public class Main {
    public static void main(String[] args) throws InterruptedException {
        ConsumerClient c = new ConsumerClient.Builder()
                .bootstrapServer("localhost:9092")
                .consumerGroup("cg")
                .consumerCount(3)
                .taskCount(3)
                .subscribedTopic("test")
                .build();

        c.start();
        ScheduledExecutorService batchExecutor = Executors.newScheduledThreadPool(1);
        batchExecutor.scheduleWithFixedDelay(new Batcher(c), 15, 30, TimeUnit.SECONDS);
    }
}
