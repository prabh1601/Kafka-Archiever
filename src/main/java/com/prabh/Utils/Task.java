package com.prabh.Utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Task implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(Task.class.getName());
    private final List<ConsumerRecord<String, String>> records;
    private volatile boolean stopped = false;
    private volatile boolean started = false;
    private volatile boolean finished = false;
    private final AtomicLong currentOffset = new AtomicLong();
    private final ReentrantLock startStopLock = new ReentrantLock();
    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    public Task(List<ConsumerRecord<String, String>> _records) {
        this.records = _records;
    }

    @Override
    public void run() {
        startStopLock.lock();
        if (stopped) return; // This happens when the task is still in executor queue
        started = true; // Task is started by executor thread pool
        startStopLock.unlock();

        for (ConsumerRecord<String, String> record : records) {
            if (stopped) break;
            // process records here
            currentOffset.set(record.offset() + 1);
        }

        finished = true;
        completion.complete(currentOffset.get());
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(currentOffset.get());
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            return -1;
        }
    }

    public boolean isFinished() {
        return finished;
    }
}
