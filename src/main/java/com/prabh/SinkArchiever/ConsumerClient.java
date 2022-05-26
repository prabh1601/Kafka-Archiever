package com.prabh.SinkArchiever;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class ConsumerClient implements ConsumerRebalanceListener {
    private final Properties consumerProperties;
    private final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);
    private final String _SUBSCRIBED_TOPIC;

    private ConsumerClient(Builder builder) {
        consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, builder._BOOTSTRAP_ID);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, builder._GROUP_ID);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, builder._AUTO_OFFSET_RESET);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this._SUBSCRIBED_TOPIC = builder._SUBSCRIBED_TOPIC;
    }

    public static class Builder {
        private String _BOOTSTRAP_ID;
        private String _GROUP_ID;
        private String _SUBSCRIBED_TOPIC;
        private String _AUTO_OFFSET_RESET;

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

        public Builder subscribe(String topic) {
            this._SUBSCRIBED_TOPIC = topic;
            return this;
        }

        public ConsumerClient build() {
            return new ConsumerClient(this);
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {

    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {

    }

    private class ConsumerThread extends Thread {
        private KafkaConsumer<String, String> consumer;
        private List<String> topicsList;

        private ConsumerThread(Builder builder) {
            Properties prop = new Properties();
        }

        public List<String> getSubscribedTopics() {
            return topicsList;
        }

        public void run() {
            if (consumer == null) {
                logger.error("Try to fix this mess :[");
                return;
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//                handleFetchedRecord();
//                checkActiveTa
            }
        }

    }
}