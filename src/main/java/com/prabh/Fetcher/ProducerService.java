package com.prabh.Fetcher;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.prabh.Utils.CompressionType;

import com.prabh.Utils.LimitedQueue;
import com.prabh.Utils.TPSCalculator;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.*;

public class ProducerService {
    private final Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private final String subscribedTopic;
    private final KafkaProducer<String, String> producer;
    private final String bootstrapId;
    private final ExecutorService executor;
    private CountDownLatch latch;
    private final RejectedRecords rejectedRecords;

    public ProducerService(String topic, String _bootstrapId, String _localDumpLocation, int producerPoolSize) {
        this.subscribedTopic = topic;
        this.bootstrapId = _bootstrapId;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("KAFKA-PRODUCER-WORKER-%d").build();
        this.executor = new ThreadPoolExecutor(producerPoolSize,
                producerPoolSize,
                0L, TimeUnit.SECONDS,
                new LimitedQueue<>(5),
                namedThreadFactory);

        this.rejectedRecords = new RejectedRecords(_localDumpLocation, createProducerClient(), topic);
        new Thread(rejectedRecords).start();
        this.producer = createProducerClient();
    }

    public void setBatchCount(int noOfBatches) {
        this.latch = new CountDownLatch(noOfBatches);
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

    public void submit(String batchName, byte[] b) {
        ProducerTask t = new ProducerTask(batchName, b);
        executor.submit(t);
    }

    public void shutdown() throws InterruptedException {
        assert latch != null;
        latch.await();
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        producer.close();
        logger.warn("Producer Service Shutdown successful");
        rejectedRecords.shutdown();
    }

    private class ProducerTask implements Runnable {
        private String filePath = null;
        private byte[] b = null;
        private String batchName;

        ProducerTask(String path) {
            this.filePath = path;
        }

        ProducerTask(String _batchName, byte[] _b) {
            this.batchName = _batchName;
            this.b = _b;
        }

        public BufferedReader getFileReader() throws IOException {
            String extension = Files.getFileExtension(filePath);
            CompressionType compressionType = CompressionType.getCompressionType(extension);
            return new BufferedReader(new InputStreamReader(
                    compressionType.wrapInputStream(new FileInputStream(filePath))));
        }

        public BufferedReader getStreamReader() throws IOException {
            String extension = Files.getFileExtension(batchName);
            CompressionType compressionType = CompressionType.getCompressionType(extension);
            return new BufferedReader(new InputStreamReader(
                    compressionType.wrapInputStream(new ByteArrayInputStream(b))));
        }

        void process(String key, String msg) {
            ProducerRecord<String, String> record = new ProducerRecord<>(subscribedTopic, key, msg);
            rejectedRecords.submit(msg, producer.send(record));
        }

        public void readlocalFile() {
            File file = new File(filePath);
            if (!file.exists()) {
                logger.error("{} Downloaded file missing in local cache", filePath);
                return;
            }

            logger.info("Loading File {}", file.getName());
            try (BufferedReader reader = getFileReader()) {
                String line;
                String key = null; // PLEASE MAKE SURE YOU WANT THIS TO BE NULL
                while ((line = reader.readLine()) != null) {
//                    tps.incrementOpCount();
                    process(key, line);
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }

            if (!file.delete()) {
                logger.error("Error Deleting file : {}", file.getName());
            }

            latch.countDown();
        }

        public void readBytes() {
            logger.info("Streaming {}", batchName);
            try (BufferedReader reader = getStreamReader()) {
                String line;
                String key = null; // MAKE SURE YOU WANT THIS TO BE NULL
                while ((line = reader.readLine()) != null) {
//                    logger.info(line);
                    process(key, line);
                }
                logger.info("Streaming complete {}", batchName);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
            latch.countDown();
        }

        public void run() {
            if (filePath == null) {
                readBytes();
            } else {
                readlocalFile();
            }
        }
    }
}