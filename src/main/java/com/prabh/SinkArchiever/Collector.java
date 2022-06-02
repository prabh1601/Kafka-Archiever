package com.prabh.SinkArchiever;

import com.amazonaws.services.customerprofiles.model.Batch;
import com.prabh.Utils.AwsClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.*;

import com.prabh.Utils.Task;

public class Collector {
    private final Logger logger = LoggerFactory.getLogger(Collector.class.getName());
    private ExecutorService taskExecutor;

    public Collector(int taskPoolSize) {
        this.taskExecutor = Executors.newFixedThreadPool(taskPoolSize);
    }

    public Task submit(List<ConsumerRecord<String, String>> records) {
        Task t = new Task(records);
        taskExecutor.submit(t);
        return t;
    }

    public void stop() {
        logger.warn("Task Executor Shutting down");
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

}
