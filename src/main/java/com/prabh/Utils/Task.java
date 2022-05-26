package com.prabh.Utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class Task implements Runnable {
    private final List<ConsumerRecord> fetchedRecords;

    public Task(List _records) {
        this.fetchedRecords = _records;
    }

    @Override
    public void run() {
        for (ConsumerRecord record : fetchedRecords) {
            // somehow upload this on s3
        }
    }
}
