package com.prabh.Fetcher;

import com.prabh.Utils.Pair;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class RejectedRecords implements Runnable {
    private batch b;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private int rejectedBatchCount = 0;
    private final Logger logger = LoggerFactory.getLogger(RejectedRecords.class);
    private final String localDumpLocation;
    private final BlockingQueue<Pair<Future<RecordMetadata>, String>> rejectedRecords;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public RejectedRecords(String _localDumpLocation, KafkaProducer<String, String> _producer, String topic) {
        this.localDumpLocation = _localDumpLocation;
        this.producer = _producer;
        this.topic = topic;
        this.rejectedRecords = new ArrayBlockingQueue<>(1000);
    }

    public void shutdown() {
        stopped.set(true);
    }

    public void submit(String record, Future<RecordMetadata> f) {
        try {
            rejectedRecords.put(new Pair<>(f, record));
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void run() {
        while (!stopped.get() || !rejectedRecords.isEmpty()) {
            Pair<Future<RecordMetadata>, String> p = rejectedRecords.poll();
            if (p != null) {
                try {
                    p.first.get();
                } catch (ExecutionException | InterruptedException e) {
                    try {
                        producer.send(new ProducerRecord<>(topic, null, p.second)).get();
                    } catch (ExecutionException ex) {
                        putToLocalCache(p.second);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        if (b != null) {
            b.flush();
        }
    }

    public void putToLocalCache(String record) {
        if (b == null || b.readyForCommit() == 2) {
            rejectedBatchCount++;
            b = new batch(rejectedBatchCount);
        }
        logger.error("Rejected Record written to local cache : {}\n", record);
        b.add(record);
    }

    private class batch {
        private final long startTime = System.currentTimeMillis();
        private long currentSize = 0;
        private final String fileName;
        private final Queue<String> buffer = new LinkedList<>();

        batch(int rejectedBatchNo) {
            this.fileName = localDumpLocation + "/Rejected/Batch" + rejectedBatchNo;
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
