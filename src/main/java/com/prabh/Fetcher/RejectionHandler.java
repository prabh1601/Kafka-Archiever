package com.prabh.Fetcher;

import com.prabh.Utils.Pair;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class RejectionHandler implements Runnable {
    private final List<String> permanentExceptions = List.of(InvalidTopicException.class.getName(),
            OffsetMetadataTooLarge.class.getName(),
            RecordTooLargeException.class.getName(),
            RecordBatchTooLargeException.class.getName(),
            UnknownServerException.class.getName());
    private batch permanentErr;
    private batch transientErr;
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Logger logger = LoggerFactory.getLogger(RejectionHandler.class);
    private final FilePaths filePaths;
    private final BlockingQueue<Pair<Future<RecordMetadata>, String>> rejectedRecords;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ProgressListener progressListener;

    public RejectionHandler(FilePaths _filePaths, KafkaProducer<String, String> _producer, String topic) {
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
        if (permanentErr != null) {
            permanentErr.flush();
        }

        if (transientErr != null) {
            transientErr.flush();
        }
    }

    public boolean isPermanentErr(Exception e) {
        return permanentExceptions.contains(e.getClass().getName());
    }

    public void retry(String msg) {
        producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e != null) {
                    putToLocalCache(msg, isPermanentErr(e));
                }
            }
        });
    }

    public void putToLocalCache(String record, boolean permanent) {
        if (progressListener != null) {
            progressListener.markRejectedRecord();
        }
        if (permanent) {
            markPermanentRejection(record);
        } else {
            markTemporaryRejection(record);
        }
    }

    public void markPermanentRejection(String record) {
        if (permanentErr == null || permanentErr.readyForCommit() == 2) {
            permanentErr = new batch(0);
        }
        logger.error("Record Rejected after final tries and put to local cache");
        permanentErr.add(record);
    }

    public void markTemporaryRejection(String record) {
        if (transientErr == null || transientErr.readyForCommit() == 2) {
            transientErr = new batch(0);
        }
        logger.error("Record Rejected after final tries and put to local cache");
        transientErr.add(record);
    }


    private class batch {
        private final long startTime = System.currentTimeMillis();
        private long currentSize = 0;
        private final String fileName;
        private final Queue<String> buffer = new LinkedList<>();

        batch(long t) {
            String dir = (t == 1) ? filePaths.RejectedDirectoryTransient : filePaths.RejectedDirectoryPermanent;
            this.fileName = dir + "/" + UUID.randomUUID();
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
                    System.out.println(s);
                    writer.println(s);
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
