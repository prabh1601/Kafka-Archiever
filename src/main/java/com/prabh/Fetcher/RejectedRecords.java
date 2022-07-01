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
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class RejectedRecords implements Runnable {
    private batch b;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Logger logger = LoggerFactory.getLogger(RejectedRecords.class);
    private final FilePaths filePaths;
    private final BlockingQueue<Pair<Future<RecordMetadata>, String>> rejectedRecords;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ProgressListener progressListener;

    public RejectedRecords(FilePaths _filePaths, KafkaProducer<String, String> _producer, String topic) {
        this.filePaths = _filePaths;
        this.producer = _producer;
        this.topic = topic;
        this.rejectedRecords = new ArrayBlockingQueue<>(1000);
    }

    public void setProgressListener(ProgressListener listener) {
        this.progressListener = listener;
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
        Thread.currentThread().setName("Rejection Handler");
        while (!stopped.get() || !rejectedRecords.isEmpty()) {
            Pair<Future<RecordMetadata>, String> p = rejectedRecords.poll();
            if (p != null) {
                try {
                    p.first.get();
                } catch (ExecutionException | InterruptedException e) {
                    retry(p.second);
                }
            }
        }
        if (b != null) {
            b.flush();
        }
    }

    public void retry(String msg) {
        try {
            producer.send(new ProducerRecord<>(topic, null, msg)).get();
        } catch (ExecutionException e) {
            putToLocalCache(msg);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void putToLocalCache(String record) {
        if (b == null || b.readyForCommit() == 2) {
            b = new batch();
            System.out.println(b.fileName);
        }
        progressListener.markFailedRecord();
        logger.error("Record Rejected after final tries and put to local cache");
        b.add(record);
    }

    private class batch {
        private final long startTime = System.currentTimeMillis();
        private long currentSize = 0;
        private final String fileName;
        private final Queue<String> buffer = new LinkedList<>();

        batch() {
            this.fileName = filePaths.RejectedDirectory + "/" + UUID.randomUUID();
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
            try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)))) {
                while (!buffer.isEmpty()) {
                    String s = buffer.poll();
                    writer.println(s);
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
