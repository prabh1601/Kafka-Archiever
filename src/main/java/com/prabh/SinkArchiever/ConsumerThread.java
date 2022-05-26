package com.prabh.SinkArchiever;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerThread implements Runnable, ConsumerRebalanceListener {
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
    private KafkaConsumer<String,String> consumer;
    private String subscribedTopic;
    private ExecutorService taskExecutor;
    private static Properties consumerProperties = new Properties();
    // Will change it to runState
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private ConsumerThread(Builder builder) {

        // set Consumer Config Properties
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, builder._BOOTSTRAP_ID);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, builder._GROUP_ID);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, builder._AUTO_OFFSET_RESET);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Assign various objects
        subscribedTopic = builder._SUBSCRIBED_TOPIC; // initialize subscribed topic
        taskExecutor = Executors.newFixedThreadPool(builder._THREAD_COUNT); // initialize executor service to decouple task management
        consumer = new KafkaConsumer<>(consumerProperties); // initialize KafkaConsumer

        // Start the Thread
        new Thread(this).start();
    }

    public static class Builder {
        private String _BOOTSTRAP_ID;
        private String _GROUP_ID;
        private String _SUBSCRIBED_TOPIC;
        private String _AUTO_OFFSET_RESET;
        private int _THREAD_COUNT;

        public Builder bootstrapServer(String serverId) {
            this._BOOTSTRAP_ID = serverId;
            return this;
        }

        public Builder consumerGroup(String groupName) {
            this._GROUP_ID = groupName;
            return this;
        }

        public Builder autoOffsetReset(String offsetPattern) {
            this._AUTO_OFFSET_RESET = offsetPattern;
            return this;
        }

        // TODO : make sure this topic exists
        public Builder subscribe(String topic) {
            this._SUBSCRIBED_TOPIC = topic;
            return this;
        }

        public Builder threadCount(int n) {
            this._THREAD_COUNT = n;
            return this;
        }

        public ConsumerThread build() {
            return new ConsumerThread(this);
        }
    }

    @Override
    public void run() {
        try {

            // Subscribe to this topic and pass this listener
            consumer.subscribe(Collections.singleton(subscribedTopic), this);
            while (!stopped.get()) {
                ConsumerRecords records = consumer.poll(Duration.ofSeconds(1));

            }

        } catch (WakeupException e) {
            logger.info("Received Shutdown signal!");
        } finally {
            consumer.close();
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }
}
