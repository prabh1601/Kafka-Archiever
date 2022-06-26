package com.prabh.Fetcher;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;

public class RejectedRecords {
    private batch b;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private int rejectedBatchCount = 0;
    private final Logger logger = LoggerFactory.getLogger(RejectedRecords.class);
    private final String localDumpLocation;

    public RejectedRecords(String _localDumpLocation, KafkaProducer<String, String> _producer, String topic) {
        this.localDumpLocation = _localDumpLocation;
        this.producer = _producer;
        this.topic = topic;
    }

    public void retry(String record, Exception e, int t) {
        int noOfRetries = 2;
        if (t >= noOfRetries) {
            if (b == null || b.readyForCommit() == 2) {
                rejectedBatchCount++;
                b = new batch(rejectedBatchCount);
            }
            logger.error("Rejected Record written to local cache : {}\n Triggered Exception : {}", record, e.getMessage());
            b.add(record);
            return;
        }

        producer.send(new ProducerRecord<>(topic, null, record), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    retry(record, e, t + 1);
                }
            }
        });
    }

    public void shutdown() {
        b.flush();
    }

    private class batch {
        private final long startTime = System.currentTimeMillis();
        private long currentSize = 0;
        private final String fileName;
        private final Queue<String> buffer = new LinkedList<>();

        batch(int rejectedBatchNo) {
            this.fileName = localDumpLocation + "Rejected/Batch" + rejectedBatchNo;
        }

        public void add(String record) {
            currentSize += record.length();
            buffer.add(record);
        }

        public int readyForCommit() {
            long maxDuration = 5 * 60 * 1000; // 5 min
            long maxSize = 5 * 1024 * 1024;// 5 MB

            int ret = 0;
            if (currentSize >= maxSize) ret = 2;
            if ((buffer.size() == 100) || (System.currentTimeMillis() - startTime > maxDuration)) ret = 1;
            if (ret != 0) flush();
            return ret;
        }

        public void flush() {
            new File(fileName).getParentFile().mkdirs();
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
                while (!buffer.isEmpty()) {
                    String s = buffer.poll();
                    writer.write(s + "\n");
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
