package com.prabh.Archiver;

import com.prabh.Utils.CompressionType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public enum PartitionBatchType {
    IN_MEMORY {
    },
    partial_memory {
    },
    LOCAL_STORAGE {
    };


    public void add(List<String> recordValues) {
    }

    public void add(String recordValue) {
    }

    void readyToCommit() {
    }

    void getObjectKey() {
    }
}
