package com.prabh.Utils;

import com.prabh.SinkArchiever.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


public class WritingTask implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(Task.class.getName());
    private final List<ConsumerRecord<String, String>> records;
    private volatile boolean stopped = false;
    private volatile boolean started = false;
    private volatile boolean finished = false;
    private final AtomicLong currentOffset = new AtomicLong();
    private final ReentrantLock startStopLock = new ReentrantLock();
    private final CompletableFuture<Long> completion = new CompletableFuture<>();

    public WritingTask(List<ConsumerRecord<String, String>> _records) {
        this.records = _records;
    }

    @Override
    public void run() {
        startStopLock.lock();
        if (stopped) return; // This happens when the task is still in executor queue
        started = true; // Task is started by executor thread pool
        startStopLock.unlock();

        log();
        try {
            final String writePath = "/mnt/Drive1/Write/";
            final int partition = records.get(0).partition();
            final String topic = records.get(0).topic();
            String fileName = writePath + "/topic" + partition + ".txt";
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
            for (ConsumerRecord<String, String> record : records) {
                if (stopped) break;
                writer.write(record.timestamp() + " - " + record.value() + "\n");
                currentOffset.set(record.offset() + 1);
            }

            writer.close();
        } catch (IOException e) {
            logger.error("Writing files abrupted");
        }

        finished = true;
        completion.complete(currentOffset.get());
    }

    private void log() {
        logger.info(Thread.currentThread().getName() + " got " + records.size() + " records from partition " + records.get(0).partition());
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
