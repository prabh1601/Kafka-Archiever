import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    Logger logger = LoggerFactory.getLogger(Producer.class.getName());

    public static void main(String[] args) {
        new Producer().run();
    }

    public void run() {

        KafkaProducer<String, String> producer = new Producer().createProducerClient();
        for (int i = 0; i < 5; i++) {
            String topic = "topic" + Integer.toString(i);
            String value = "Just trying to have a tweet %d".formatted(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Successfully pushed data into Kafka");
                    }
                }
            });
        }

        producer.close();
    }

    public KafkaProducer<String, String> createProducerClient() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(prop);
    }


}
