package com.prabh.SinkArchiever;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class WriterClient {
    private final Logger logger = LoggerFactory.getLogger(WriterClient.class.getName());
    private final ExecutorService taskExecutor;
    private final List<ConcurrentHashMap<TopicPartition, WritingTask>> activeTasks;
    private final UploaderClient uploader = new UploaderClient();

    public WriterClient(int noOfConsumers, int taskPoolSize) {
        this.taskExecutor = Executors.newFixedThreadPool(taskPoolSize);
        activeTasks = new ArrayList<>(noOfConsumers);
        for (int i = 0; i < noOfConsumers; i++) {
            activeTasks.add(new ConcurrentHashMap<>());
        }
    }

    public WritingTask submit(int consumer, TopicPartition partition, List<ConsumerRecord<String, String>> records) {
        WritingTask t = new WritingTask(records);
        taskExecutor.submit(t);
        activeTasks.get(consumer).put(partition, t);
        return t;
    }

    public Map<TopicPartition, OffsetAndMetadata> checkActiveTasks(int consumer) {
        Map<TopicPartition, OffsetAndMetadata> donePartitions = new HashMap<>();
        List<TopicPartition> tasksToRemove = new ArrayList<>();
        activeTasks.get(consumer).forEach((currentPartition, task) -> {
            long offset = task.getCurrentOffset();
            if (task.isFinished() && offset > 0) {
                donePartitions.put(currentPartition, new OffsetAndMetadata(offset));
                tasksToRemove.add(currentPartition);
            }
        });

        tasksToRemove.forEach(activeTasks.get(consumer)::remove);
        return donePartitions;
    }

    public Map<TopicPartition, OffsetAndMetadata> handleRevokedPartitionTasks(int consumer, Collection<TopicPartition> partitions) {

        // 1. Fetch the tasks of revoked partitions and stop them
        Map<TopicPartition, WritingTask> revokedTasks = new HashMap<>();
        partitions.forEach(currentPartition -> {
            WritingTask task = activeTasks.get(consumer).remove(currentPartition);
            if (task != null) {
                task.stop();
                revokedTasks.put(currentPartition, task);
            }
        });

        // 2. Wait for revoked tasks to complete processing current record and fetch offsets
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        revokedTasks.forEach((currentPartition, task) -> {
            long offset = task.waitForCompletion();
            if (offset > 0) {
                revokedPartitionOffsets.put(currentPartition, new OffsetAndMetadata(offset));
            }
        });

        return revokedPartitionOffsets;
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

    private class WritingTask implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(Task.class.getName());
        private final List<ConsumerRecord<String, String>> records;
        private volatile boolean stopped = false;
        private volatile boolean started = false;
        private volatile boolean finished = false;
        private final AtomicLong currentOffset = new AtomicLong();
        private final ReentrantLock startStopLock = new ReentrantLock();
        private final CompletableFuture<Long> completion = new CompletableFuture<>();
        private final int chunkSize = 10;
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
                final String writePath = "/mnt/Drive1/Kafka-Dump/";
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
                long fileSizeInMB = Files.size(Paths.get(fileName)) / (1024 * 1024);
                if(fileSizeInMB > chunkSize) {
                    stageForUpload(fileName);
                }
            } catch (IOException e) {
                logger.error("Writing files abrupted");
            }

            finished = true;
            completion.complete(currentOffset.get());
        }

        private void log() {
            logger.info(Thread.currentThread().getName() + " got " + records.size() + " records from partition " + records.get(0).partition());
        }

        void stageForUpload(String fileName) {
            uploader.upload(fileName);
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


}
