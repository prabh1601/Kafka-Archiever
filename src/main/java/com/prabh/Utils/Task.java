package com.prabh.Utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Task implements Runnable {
    private final List<ConsumerRecord> fetchedRecords;
    private Logger logger = LoggerFactory.getLogger(Task.class.getName());
    private AtomicLong currentOffset = new AtomicLong();

    public Task(List _records) {
        this.fetchedRecords = _records;
    }

    @Override
    public void run() {
        for (ConsumerRecord record : fetchedRecords) {
        }
    }

    public long getCurrentOffset(){
        return current
    }
}
