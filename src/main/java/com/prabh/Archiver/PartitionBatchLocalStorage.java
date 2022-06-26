package com.prabh.Archiver;

import com.prabh.Utils.CompressionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionBatchLocalStorage {
    private final Logger logger = LoggerFactory.getLogger(PartitionBatchLocalStorage.class);
    private final ConsumerRecord<String, String> firstRecord;
    private ConsumerRecord<String, String> latestRecord;
    private final String localDumpLocation = String.format("%s/KafkaToS3", System.getProperty("java.io.tmpdir"));
    private final CompressionType compressionType;

    PartitionBatchLocalStorage(ConsumerRecord<String, String> record, CompressionType _compressionType) {
        this.compressionType = _compressionType;
        this.firstRecord = record;
    }
}
