package com.prabh.Archiver;

import TestingTools.TPSCalculator;
import com.prabh.Utils.CompressionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TopicPartitionWriter {
    private static final Logger logger = LoggerFactory.getLogger(TopicPartitionWriter.class);
    private final CompressionType compressionType;
    private final ConsumerRecord<String, String> leaderRecord;
    private final String filePath;
    private ConsumerRecord<String, String> latestRecord;
    String localDumpLocation = String.format("%s/KafkaToS3", System.getProperty("java.io.tmpdir"));
    private final long startTime = System.currentTimeMillis();
    private boolean opened = false;
    private long remBatchSizeInBytes = 10 * 1024 * 1024; // 10 MB
    private static TPSCalculator tps = new TPSCalculator().start(5L, TimeUnit.SECONDS, new TPSCalculator.AbstractTPSCallback() {
        @Override
        public void tpsStat(TPSCalculator.TPSStat stat) {
            logger.error("stats: " + stat.toString());
        }
    });

    public TopicPartitionWriter(ConsumerRecord<String, String> _leaderRecord, CompressionType _compressionType) {
        this.leaderRecord = _leaderRecord;
        this.compressionType = _compressionType;
        int partition = leaderRecord.partition();
        long startingOffset = leaderRecord.offset();
        this.filePath = localDumpLocation + "/" + partition + "_" + startingOffset;
    }

    PrintWriter getWriter() {
        try {
            new File(localDumpLocation).mkdirs();
            return new PrintWriter(
                    new BufferedOutputStream(
                            compressionType.wrapOutputStream(
                                    new FileOutputStream(filePath, true))));
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    public void addToBuffer(List<ConsumerRecord<String, String>> records) {
        opened = true;
        try (PrintWriter writer = getWriter()) {
            for (ConsumerRecord<String, String> record : records) {
                writer.println(record.value());
                tps.incrementOpCount();
                remBatchSizeInBytes -= (record.serializedValueSize() + 1);
                latestRecord = record;
            }
        }
    }

    long remainingSpace() {
        return remBatchSizeInBytes;
    }

    public boolean readyForCommit() {
        if (!opened) return false;

        // Check Chunk Size
        if (remBatchSizeInBytes <= 0) return true;

        // Check batch timegap  and batch open duration
        long timeGapInMillis = getLastTimeStamp() - getFirstTimeStamp();
        long durationInMillis = System.currentTimeMillis() - startTime;

        long maxBatchDurationInMillis = 5 * 60 * 1000;
        return Math.max(timeGapInMillis, durationInMillis) >= maxBatchDurationInMillis;
    }

    long getFirstTimeStamp() {
        return leaderRecord.timestamp();
    }

    long getLastTimeStamp() {
        return latestRecord.timestamp();
    }


    String getKey() {
        int partition = leaderRecord.partition();
        long startingOffset = leaderRecord.offset();
        long endingOffset = latestRecord.offset();
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(getFirstTimeStamp());


        String fileName = partition + "_" + startingOffset + "_" + endingOffset;
        if (!compressionType.extension.equals("")) {
            fileName += "." + compressionType.extension;
        }
        return "topics/" + leaderRecord.topic() + "/" + c.get(Calendar.YEAR) + "/" + (c.get(Calendar.MONTH) + 1) + "/"
                + c.get(Calendar.DAY_OF_MONTH) + "/" + c.get(Calendar.HOUR_OF_DAY) + "/" + c.get(Calendar.MINUTE) + "/" + fileName;
    }

    String getFilePath() {
        return filePath;
    }
}
