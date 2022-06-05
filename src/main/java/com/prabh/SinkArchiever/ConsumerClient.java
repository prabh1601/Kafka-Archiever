package com.prabh.SinkArchiever;

import com.prabh.Utils.AdminController;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerClient {
    private final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);
    private final List<ConsumerThread> consumers;
    private final Builder builder;
    private final AdminController admin;
    private WriterClient writer;

    private ConsumerClient(Builder _builder) {
        this.builder = _builder;
        this.consumers = new ArrayList<>(builder.noOfConsumers);
        this.admin = new AdminController(builder.serverId);
    }

    public void start() {
        if (!admin.exists(builder.topic)) {
            logger.info("Consumer Group Start failed due to subscribing nonexistent topic");
            return;
        }
        this.writer = new WriterClient(builder.noOfConsumers, builder.noOfSimultaneousTask);
        for (int i = 0; i < builder.noOfConsumers; i++) {
            ConsumerThread c = new ConsumerThread();
            c.setConsumerDetails(i);
            c.start();
            synchronized (consumers) {
                consumers.add(c);
            }
        }
    }

    public void shutdown() {
        writer.stop();
        synchronized (consumers) {
            consumers.forEach(ConsumerThread::stopConsumer);
            consumers.clear();
        }
        logger.info("Complete Shutdown Successful");
    }

    public static class Builder {
        public String serverId;
        public int noOfConsumers;
        public int noOfSimultaneousTask;
        public String groupName;
        public String topic;

        public Builder() {

        }

        // Server to connect
        public Builder bootstrapServer(String _serverId) {
            this.serverId = _serverId;
            return this;
        }

        // Name of Consumer Group for the service
        public Builder consumerGroup(String _groupName) {
            this.groupName = _groupName;
            return this;
        }

        // No of Consumers in the consumer Group
        public Builder consumerCount(int _noOfConsumers) {
            this.noOfConsumers = _noOfConsumers;
            return this;
        }

        // No of threads for ExecutorService
        public Builder taskCount(int _noOfSimultaneousTask) {
            this.noOfSimultaneousTask = _noOfSimultaneousTask;
            return this;
        }

        // Make sure this topic Exists
        public Builder subscribedTopic(String _topic) {
            this.topic = _topic;
            return this;
        }

        public ConsumerClient build() {
            return new ConsumerClient(this);
        }
    }

    private class ConsumerThread extends Thread implements ConsumerRebalanceListener {
        private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        private final KafkaConsumer<String, String> consumer;
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
        private final String subscribedTopic;
        private String consumerName;
        private int consumerNo;
        private long lastCommitTime = System.currentTimeMillis();

        public ConsumerThread() {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.serverId);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, builder.groupName);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000000);
            this.consumer = new KafkaConsumer<>(consumerProperties);
            this.subscribedTopic = builder.topic;
        }

        @Override
        public void run() {
            try {
                logger.info(String.format("%s Started", consumerName));

                consumer.subscribe(List.of(subscribedTopic), this);
                while (!stopped.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        log(records);
                        handleFetchedRecords(records);
                    }
                    checkActiveTasks();
                    commitOffsets();
                }
            } catch (WakeupException e) {
                if (!stopped.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
                logger.warn(String.format("%s Shutdown successfully", consumerName));
            }
        }

        public void handleFetchedRecords(ConsumerRecords<String, String> records) {
            List<TopicPartition> partitionsToPause = new ArrayList<>();
            records.partitions().forEach(currentPartition -> {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(currentPartition);
                writer.submit(consumerNo, currentPartition, partitionRecords);
                partitionsToPause.add(currentPartition);
            });

            consumer.pause(partitionsToPause);
        }

        public void checkActiveTasks() {
            List<TopicPartition> partitionsToResume = new ArrayList<>();
            Map<TopicPartition, OffsetAndMetadata> doneTasks = writer.checkActiveTasks(consumerNo);
            doneTasks.forEach((currentPartition, offsets) -> {
                partitionsToResume.add(currentPartition);
                pendingOffsets.put(currentPartition, offsets);
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
                logger.error("{} failed to commit offsets during routine offset commit", consumerName);
            }

        }

        public void log(ConsumerRecords<String, String> records) {
            logger.info("{} Fetched {} records constituting of {}", consumerName, records.count(), records.partitions());
        }

        private void setConsumerDetails(int consumerNumber) {
            this.consumerName = "CONSUMER_THREAD-" + this.subscribedTopic + "-" + consumerNumber;
            this.consumerNo = consumerNumber;
            Thread.currentThread().setName(this.consumerName);
        }

        public void stopConsumer() {
            logger.warn("Stopping {}", consumerName);
            stopped.set(true);
            consumer.wakeup();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            writer.handleRevokedPartitionTasks(consumerNo, partitions);

            Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = writer.handleRevokedPartitionTasks(consumerNo, partitions);
            pendingOffsets.putAll(revokedPartitionOffsets);

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
                logger.error("{} Failed to commit offset during re-balance", consumerName);
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            consumer.resume(partitions);
        }
    }
}
