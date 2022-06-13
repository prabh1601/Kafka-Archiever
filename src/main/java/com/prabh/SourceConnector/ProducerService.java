package com.prabh.SourceConnector;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.prabh.Utils.Config;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerService {
    private final Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private final String subscribedTopic;
    private final KafkaProducer<String, String> producer;
    private final String bootstrapId;
    private final ExecutorService executor;
    // ^ This is not final. Still pending benchmarks

    public ProducerService(String topic, String _bootstrapId) {
        this.subscribedTopic = topic;
        this.bootstrapId = _bootstrapId;
        this.executor = Executors.newFixedThreadPool(Config.ProducerCount,
                new ThreadFactoryBuilder().setNameFormat("PRODUCER-THREAD-%d").build());
        this.producer = createProducerClient();
    }

    public KafkaProducer<String, String> createProducerClient() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapId);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(prop);
    }

    public void submit(String filePath) {
        ProducerTask t = new ProducerTask(filePath);
        executor.submit(t);
    }

    public void shutdown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        producer.close();
        logger.info("Producer Service Shutdown successful");
    }

    private class ProducerTask implements Runnable {
        private final String filePath;

        ProducerTask(String path) {
            this.filePath = path;
        }

        void process(String line) {
            String key = null;
            // This will go into infinite loop if subscribedTopic is null or nonexistant
            ProducerRecord<String, String> record = new ProducerRecord<>(subscribedTopic, key, line);
            producer.send(record);
        }

        public void run() {
            File file = new File(filePath);
            if (!file.exists()) {
                logger.error("{} Downloaded file missing in local cache", filePath);
            }

            try {
                FileReader frd = new FileReader(filePath);
                BufferedReader brd = new BufferedReader(frd);
                String line;
                while ((line = brd.readLine()) != null) {
                    process(line);
                }
                brd.close();
                frd.close();
//                file.delete();
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
