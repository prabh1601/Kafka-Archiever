package com.prabh.SinkArchiever;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerClient {
    private final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);
    private final List<ConsumerThread> consumers;
    private final WriterClient writer;
    private final CountDownLatch runningStatus;
    private final int noOfConsumers;
    private final String groupName;
    private final String serverId;
    private final String subscribedTopic;

    ConsumerClient(WriterClient _writer, int _noOfConsumers, String _groupName, String _serverId, String _topic) {
        this.writer = _writer;
        this.groupName = _groupName;
        this.serverId = _serverId;
        this.noOfConsumers = _noOfConsumers;
        this.runningStatus = new CountDownLatch(this.noOfConsumers);
        this.consumers = new ArrayList<>(_noOfConsumers);
        this.subscribedTopic = _topic;
    }

    public void start() {
        for (int i = 0; i < noOfConsumers; i++) {
            ConsumerThread c = new ConsumerThread();
            c.setConsumerDetails(i);
            c.start();
            synchronized (consumers) {
                consumers.add(c);
            }
        }
    }

    public void shutdown() {
        synchronized (consumers) {
            consumers.forEach(ConsumerThread::stopConsumer);
            consumers.clear();
        }
        try {
            runningStatus.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        logger.warn("Consumer Client Shutdown Complete");
    }

    private class ConsumerThread extends Thread implements ConsumerRebalanceListener {
        private final Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        private final KafkaConsumer<String, String> consumer;
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
        private String consumerName;
        private int consumerNo;
        private long lastCommitTime = System.currentTimeMillis();

        public ConsumerThread() {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverId);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000000);
            this.consumer = new KafkaConsumer<>(consumerProperties);
        }

        @Override
        public void run() {
            try {
                logger.info("{} Started", consumerName);

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
                runningStatus.countDown();
                logger.warn("{} Shutdown Successfully", consumerName);
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
            if (!partitionsToResume.isEmpty()) {
                consumer.resume(partitionsToResume);
            }
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
            this.consumerName = "CONSUMER_THREAD-" + subscribedTopic + "-" + (consumerNumber + 1);
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
