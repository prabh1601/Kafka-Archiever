package com.prabh.Archiver;

import com.prabh.Utils.CompressionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Calendar;

public class PartitionBatchInMemory {
    private final Logger logger = LoggerFactory.getLogger(PartitionBatchInMemory.class);
    private final CompressionType compressionType;
    private final BufferedOutputStream out;
    private final ConsumerRecord<String, String> leaderRecord;
    private ConsumerRecord<String, String> latestRecord;
    private final int maxBatchingSize = 6 * 1024 * 1024;
    private int currentBatchSize = 0;
    private final ByteArrayOutputStream bStream = new ByteArrayOutputStream(maxBatchingSize);

    public PartitionBatchInMemory(ConsumerRecord<String, String> _leaderRecord, CompressionType _compressionType) {
        this.leaderRecord = _leaderRecord;
        this.compressionType = _compressionType;
        this.out = getOutputStream();
    }

    BufferedOutputStream getOutputStream() {
        try {
            return new BufferedOutputStream(compressionType.wrapOutputStream(bStream));
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    public void addToBuffer(ConsumerRecord<String, String> record) {
        latestRecord = record;
        try {
            String s = record.value() + "\n";
            out.write(s.getBytes());
            currentBatchSize += (record.serializedValueSize() + 1);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public boolean readyForCommit() {
        long maxBatchDurationInMillis = 5 * 60 * 1000; // 5 Min
        long maxBatchSizeInBytes = 5 * 1024 * 1024; // 5 MB
        if (currentBatchSize == 0) return false;

        // Check Chunk Duration
        long durationInMillis = getLastTimeStamp() - getFirstTimeStamp();
        if (durationInMillis > maxBatchDurationInMillis) return true;

        // Check Chunk Size
        if (currentBatchSize > maxBatchSizeInBytes) return true;

        return false;
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

    public byte[] commitBatch() {
        try {
            bStream.close();
            out.close();
            return bStream.toByteArray();
        } catch (IOException e) {
            logger.error(e.getMessage());
            return null;
        }
    }
}
