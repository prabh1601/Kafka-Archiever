package com.prabh.Fetcher;

import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.prabh.Utils.CompressionType;

import com.prabh.Utils.LimitedQueue;
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
    private final RejectionHandler rejectedRecords;
    private ProgressListener progressListener;
    private final CountDownLatch completion;

    ProducerService(String topic, String _bootstrapId, FilePaths filePaths,CountDownLatch completion, int producerPoolSize) {
        this.completion = completion;
        this.subscribedTopic = topic;
        this.bootstrapId = _bootstrapId;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("KAFKA-PRODUCER-WORKER-%d").build();
        this.executor = new ThreadPoolExecutor(producerPoolSize,
                producerPoolSize,
                0L, TimeUnit.SECONDS,
                new LimitedQueue<>(10),
                namedThreadFactory);

        this.rejectedRecords = new RejectionHandler(filePaths, createProducerClient(), topic);
        new Thread(rejectedRecords).start();
        this.producer = createProducerClient();
        sendInitialSingal();
    }

    void setProgressListener(ProgressListener listener) {
        this.progressListener = listener;
        rejectedRecords.setProgressListener(progressListener);
    }

    public KafkaProducer<String, String> createProducerClient() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapId);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        prop.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100");
        return new KafkaProducer<>(prop);
    }

    public void sendInitialSingal() {
        logger.info("Sending Initial Signal");
        try {
            producer.send(new ProducerRecord<>(subscribedTopic, null, "InitialSignal")).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Kafka Initial Signal Failed - " + e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    public void submit(String objectKey, String filePath) {
        ProducerTask t = new ProducerTask(objectKey, filePath);
        executor.submit(t);
    }

    public void submit(String objectKey, String batchName, byte[] b) {
        ProducerTask t = new ProducerTask(objectKey, batchName, b);
        executor.submit(t);
    }

    public void shutdown() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        producer.close();
        rejectedRecords.shutdown();
        progressListener.stop();
        completion.countDown();
        logger.warn("Producer Service Shutdown successful");
    }

    private class ProducerTask implements Runnable {
        private final String objectKey;
        private String filePath = null;
        private byte[] b = null;
        private String batchName;

        ProducerTask(String key, String path) {
            this.objectKey = key;
            this.filePath = path;
        }

        ProducerTask(String key, String _batchName, byte[] _b) {
            this.objectKey = key;
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

            logger.info("Loading File {} to Kafka", file.getName());
            try (BufferedReader reader = getFileReader()) {
                String line;
                String key = null; // PLEASE MAKE SURE YOU WANT THIS TO BE NULL
                while ((line = reader.readLine()) != null) {
                    process(key, line);
                    progressListener.markProducedRecord();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }

            progressListener.markProducedObject(objectKey);
            if (!file.delete()) {
                logger.error("Error Deleting file : {}", file.getName());
            }
        }

        public void readBytes() {
            logger.info("Streaming {} ", batchName);
            try (BufferedReader reader = getStreamReader()) {
                String line;
                String key = null; // MAKE SURE YOU WANT THIS TO BE NULL
                while ((line = reader.readLine()) != null) {
                    process(key, line);
                    progressListener.markProducedRecord();
                }
                progressListener.markProducedObject(objectKey);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
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