package com.prabh.SinkArchiever;

import com.prabh.Utils.Task;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerThread extends Thread implements ConsumerRebalanceListener {
    private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Map<TopicPartition, Task> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
    private final Collector collector;
    private final String subscribedTopic;
    private String consumerName;
    private long lastCommitTime = System.currentTimeMillis();

    public ConsumerThread(ConsumerClient.Builder builder) {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.serverId);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, builder.groupName);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.subscribedTopic = builder.topic;
        this.collector = new Collector(builder.noOfSimultaneousTask);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(subscribedTopic), this);
            while (!stopped.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    handleFetchedRecords(records);
                }
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException e) {
            if (!stopped.get()) {
                throw e;
            }
            logger.warn(String.format("%s Shutting down !!", consumerName));
        } finally {
            consumer.close();
        }
    }

    public void handleFetchedRecords(ConsumerRecords<String, String> records) {
        List<TopicPartition> partitionsToPause = new ArrayList<>();
        records.partitions().forEach(currentPartition -> {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(currentPartition);
            Task t = collector.submit(partitionRecords);
            partitionsToPause.add(currentPartition);
            activeTasks.put(currentPartition, t);
        });

        consumer.pause(partitionsToPause);
    }

    public void checkActiveTasks() {
        List<TopicPartition> partitionsToResume = new ArrayList<>();
        activeTasks.forEach((currentPartition, task) -> {
            if (task.isFinished()) {
                partitionsToResume.add(currentPartition);
            }
            long offset = task.getCurrentOffset();
            if (offset > 0) {
                pendingOffsets.put(currentPartition, new OffsetAndMetadata(offset));
            }
        });

        consumer.resume(partitionsToResume);
    }

    public void commitOffsets() {
        try {
            long currentTimeInMillis = System.currentTimeMillis();
            if (currentTimeInMillis - lastCommitTime > 5000) {
                if (!pendingOffsets.isEmpty()) {
                    consumer.commitSync(pendingOffsets);
                    pendingOffsets.clear();
                }
                lastCommitTime = currentTimeInMillis;
            }
        } catch (Exception e) {
            logger.error(String.format("%s failed to commit offsets during routine offset commit", consumerName));
        }

    }

    public void setName(int consumerNumber) {
        try {
            consumerName = "CONSUMER_THREAD-" + subscribedTopic + consumerNumber;
            Thread.currentThread().setName(consumerName);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 1. Fetch the tasks of revoked partitions and stop them
        Map<TopicPartition, Task> revokedTasks = new HashMap<>();
        partitions.forEach(currentPartition -> {
            Task task = activeTasks.remove(currentPartition);
            if (task != null) {
                task.stop();
                revokedTasks.put(currentPartition, task);
            }
        });

        // 2. Wait for revoked tasks to complete processing current record and fetch offsets
        revokedTasks.forEach((currentPartition, task) -> {
            long offset = task.waitForCompletion();
            if (offset > 0) {
                pendingOffsets.put(currentPartition, new OffsetAndMetadata(offset));
            }
        });

        // 3. Get the offsets of revoked partitions
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        partitions.forEach(currentPartition -> {
            OffsetAndMetadata offset = pendingOffsets.remove(currentPartition);
            if (offset != null) {
                offsetsToCommit.put(currentPartition, offset);
            }
        });

        // 4. Commit offsets of revoked partitions
        try {
            consumer.commitSync(offsetsToCommit);
        } catch (Exception e) {
            logger.error(String.format("%s Failed to commit offset during rebalance", consumerName));
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.resume(partitions);
    }
}