//package com.prabh.Archiver;
//
//import com.prabh.Utils.CompressionType;
//import com.prabh.Utils.Config;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.util.Calendar;
//import java.util.LinkedList;
//import java.util.Queue;
//
//public class PartitionBatchPartialMemory {
//    private final Logger logger = LoggerFactory.getLogger(PartitionBatchPartialMemory.class);
//    private final Queue<ConsumerRecord<String, String>> buffer = new LinkedList<>();
//    private long chunkSize = 0L;
//    private final ConsumerRecord<String, String> leaderRecord;
//    private ConsumerRecord<String, String> latestRecord;
//    private final CompressionType compressionType;
//    private final String writeDir;
//
//    public PartitionBatchPartialMemory(ConsumerRecord<String, String> _leaderRecord, CompressionType _compressionType) {
//        this.leaderRecord = _leaderRecord;
//        this.writeDir = + "/" + leaderRecord.topic();
//        this.compressionType = _compressionType;
//    }
//
//    public void addToBuffer(ConsumerRecord<String, String> record) {
//        latestRecord = record;
//        buffer.add(record);
//        chunkSize += record.serializedValueSize();
//    }
//
//    public boolean readyForCommit() {
//        if (buffer.isEmpty()) return false;
//
//        // Check Chunk Duration
//        long durationInMillis = getLastTimeStamp() - getFirstTimeStamp();
//        if (durationInMillis > ) return true;
//
//        // Check Chunk Size
//        if (chunkSize > Config.maxChunkSizeInBytes) return true;
//
//        return false;
//    }
//
//    long getFirstTimeStamp() {
//        return leaderRecord.timestamp();
//    }
//
//    long getLastTimeStamp() {
//        return latestRecord.timestamp();
//    }
//
//    String getKeyPrefix() {
//        Calendar c = Calendar.getInstance();
//        long timeStampInMillis = getFirstTimeStamp();
//        c.setTimeInMillis(timeStampInMillis);
//        return "topics/" + leaderRecord.topic() + "/" + c.get(Calendar.YEAR) + "/" + (c.get(Calendar.MONTH) + 1) + "/"
//                + c.get(Calendar.DAY_OF_MONTH) + "/" + c.get(Calendar.HOUR_OF_DAY);
//    }
//
//    public String getFileName() throws NullPointerException {
//        long startingOffset = leaderRecord.offset();
//        long timestampInMillis = getFirstTimeStamp();
//        int partition = leaderRecord.partition();
//        Calendar c = Calendar.getInstance();
//        c.setTimeInMillis(timestampInMillis);
//        String fileName = c.get(Calendar.MINUTE) + "_" + partition + "_" + startingOffset;
//        if (!compressionType.extension.equals("")) {
//            fileName += "." + compressionType.extension;
//        }
//
//        return fileName;
//    }
//
//    public String getFilePath(boolean makeDirs) {
//        if (makeDirs) {
//            new File(writeDir).mkdirs();
//        }
//        return writeDir + "/" + getFileName();
//    }
//
//    PrintWriter getWriter() throws IOException {
//        String filePath = getFilePath(true);
//        return new PrintWriter(
//                compressionType.wrapOutputStream(
//                        new BufferedOutputStream(
//                                new FileOutputStream(filePath, true))));
//    }
//
//    public void commitBatch() {
//        try (PrintWriter writer = getWriter()) {
//            while (!buffer.isEmpty()) {
//                ConsumerRecord<String, String> record = buffer.poll();
//                writer.println(record.value());
//            }
//        } catch (IOException e) {
//            logger.error(e.getMessage(), e);
//        }
//    }
//}