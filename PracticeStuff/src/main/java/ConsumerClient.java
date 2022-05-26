
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

public class ConsumerClient implements ConsumerRebalanceListener {
    Properties prop;
    private final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);
    private String _SUBSCRIBED_TOPIC;

    private ConsumerClient(Builder builder) {
        prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, builder._BOOTSTRAP_ID);
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, builder._GROUP_ID);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, builder._AUTO_OFFSET_RESET);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
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

        private ConsumerThread() {
            this.consumer = new KafkaConsumer<String, String>(prop);
        }

        void handleFetchedRecord(ConsumerRecords<String, String> records) {

        }

        void checkActiveTasks() {

        }

        void commitOffset() {

        }

        public void run() {
            if (consumer == null) {
                logger.error("Try to fix this mess :[");
                return;
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                handleFetchedRecord(records);
                checkActiveTasks();
                commitOffset();
            }
        }

    }
}