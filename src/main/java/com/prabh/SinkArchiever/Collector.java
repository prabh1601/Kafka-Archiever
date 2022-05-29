package com.prabh.SinkArchiever;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.prabh.Utils.Task;

public class Collector {
    private final Logger logger = LoggerFactory.getLogger(Collector.class.getName());
    private final ExecutorService taskExecutor;

    Collector(int size) {
        taskExecutor = Executors.newFixedThreadPool(size);
    }

    public Task submit(List<ConsumerRecord<String, String>> records) {
        Task t = new Task(records);
        taskExecutor.submit(t);
        return t;
    }

    public void stop(){
        logger.warn("Task Executor Shutting down");
        taskExecutor.shutdown();
    }
}
