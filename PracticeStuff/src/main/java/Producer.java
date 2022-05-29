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
        int n = (int) 1e7;
        for (int i = 0; i < n; i++) {
            String topic = "test";
            String value = "Just trying to have a tweet %d".formatted(i);
            final String callBackResponse = "Successfully pushed %d-th string".formatted(i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e == null) {
//                        logger.info(callBackResponse);
//                    }
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
