package com.prabh.Archiver;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.prabh.Utils.CompressionType;
import com.prabh.Utils.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class WriterService {
    private final Logger logger = LoggerFactory.getLogger(WriterService.class);
    private final ExecutorService taskExecutor;
    private final List<ConcurrentHashMap<TopicPartition, WritingTask>> activeTasks;
    private final ConcurrentHashMap<TopicPartition, Queue<ConsumerRecord<String, String>>> currentBatchBuffer = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicPartition, Long> currentBatchSize = new ConcurrentHashMap<>();
    private final UploaderService uploader;
    private final String compressionTypeName;

    WriterService(int noOfConsumers, int taskPoolSize, String _compressionType, UploaderService _uploader) {
        this.uploader = _uploader;
        this.compressionTypeName = _compressionType;
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("WRITER-THREAD-%d").build();
        this.taskExecutor = Executors.newFixedThreadPool(taskPoolSize, namedThreadFactory);
        activeTasks = new ArrayList<>(noOfConsumers);
        for (int i = 0; i < noOfConsumers; i++) {
            activeTasks.add(new ConcurrentHashMap<>());
        }
    }

    public void submit(int consumer, TopicPartition partition, List<ConsumerRecord<String, String>> records) {
        WritingTask t = new WritingTask(records, partition);
        taskExecutor.submit(t);
        activeTasks.get(consumer).put(partition, t);
    }

    public Map<TopicPartition, OffsetAndMetadata> checkActiveTasks(int consumer) {
        Map<TopicPartition, OffsetAndMetadata> donePartitions = new HashMap<>();
        List<TopicPartition> tasksToRemove = new ArrayList<>();
        activeTasks.get(consumer).forEach((currentPartition, task) -> {
            long offset = task.getCurrentOffset();
            if (task.isFinished() && offset > 0) {
                donePartitions.put(currentPartition, new OffsetAndMetadata(offset));
                tasksToRemove.add(currentPartition);
            }
        });

        tasksToRemove.forEach(activeTasks.get(consumer)::remove);
        return donePartitions;
    }

    public Map<TopicPartition, OffsetAndMetadata> handleRevokedPartitionTasks(int consumer, Collection<TopicPartition> partitions) {

        // 1. Fetch the tasks of revoked partitions and stop them
        Map<TopicPartition, WritingTask> revokedTasks = new HashMap<>();
        partitions.forEach(currentPartition -> {
            WritingTask task = activeTasks.get(consumer).remove(currentPartition);
            if (task != null) {
                task.stop();
                revokedTasks.put(currentPartition, task);
            }
        });

        // 2. Wait for revoked tasks to complete processing current record and fetch offsets
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        revokedTasks.forEach((currentPartition, task) -> {
            long offset = task.waitForCompletion();
            if (offset > 0) {
                revokedPartitionOffsets.put(currentPartition, new OffsetAndMetadata(offset));
            }
        });

        return revokedPartitionOffsets;
    }

    public void shutdown() {
        taskExecutor.shutdown();
        try {
            taskExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        logger.warn("Writing Client Shutdown complete");
    }

    private class WritingTask implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(WritingTask.class.getName());
        private final List<ConsumerRecord<String, String>> records;
        private final TopicPartition partition;
        private volatile boolean stopped = false;
        private volatile boolean started = false;
        private volatile boolean finished = false;
        private final AtomicLong currentOffset = new AtomicLong();
        private final ReentrantLock startStopLock = new ReentrantLock();
        private final CompletableFuture<Long> completion = new CompletableFuture<>();
        private final CompressionType compressionType = CompressionType.getCompressionType(compressionTypeName);
        private final String writeDir;

        public WritingTask(List<ConsumerRecord<String, String>> _records, TopicPartition _partition) {
            this.records = _records;
            this.partition = _partition;
            this.writeDir = Config.WriterServiceDir + "/" + partition.topic();
            new File(writeDir).mkdirs();
            if (!currentBatchBuffer.containsKey(partition)) {
                currentBatchBuffer.put(partition, new LinkedList<>());
                currentBatchSize.put(partition, (long) 0);
            }
        }

        public long getBatchTimeStamp() throws NullPointerException {
            return Objects.requireNonNull(currentBatchBuffer.get(partition).peek()).timestamp();
        }

        public String getBatchFileName() throws NullPointerException {
            long startingOffset = Objects.requireNonNull(currentBatchBuffer.get(partition).peek()).offset();
            long timestampInMillis = getBatchTimeStamp();
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(timestampInMillis);
            return c.get(Calendar.MINUTE) + "_" + partition.partition() + "_" + startingOffset + "." + compressionType.extension;
        }

        // Still pending Benchmarks and optimizations
        PrintWriter getWriter() throws IOException {
            String fileName = getBatchFileName();
            String filePath = writeDir + "/" + fileName;
            return new PrintWriter(
                    compressionType.wrapOutputStream(
                            new BufferedOutputStream(
                                    new FileOutputStream(filePath, true))));
        }

        public String commitBatch() {
            currentBatchSize.put(partition, (long) 0);
            String fileName = getBatchFileName();
            Queue<ConsumerRecord<String, String>> batch = currentBatchBuffer.get(partition);

            try (PrintWriter writer = getWriter()) {
                while (!batch.isEmpty()) {
                    ConsumerRecord<String, String> record = batch.poll();
                    writer.println(record.value());
                }

            } catch (IOException e) {
                logger.info("Still pending IOException Handling");
            }

            return fileName;
        }


        boolean readyForCommit(long currentRecordStamp) throws IOException {
            Queue<ConsumerRecord<String, String>> batch = currentBatchBuffer.get(partition);
            if (batch.isEmpty()) return false;

            // Check Chunk Duration
            long startRecordStamp = batch.peek().timestamp();
            long durationInMillis = currentRecordStamp - startRecordStamp;
            if (durationInMillis > Config.maxBatchDurationInMillis) return true;

            // Check Chunk Size
            if (currentBatchSize.get(partition) > Config.maxChunkSizeInBytes) return true;

            return false;
        }

        public void addToBuffer(ConsumerRecord<String, String> record) {
            currentBatchBuffer.get(partition).add(record);
            long recordSize = record.serializedValueSize();
            currentBatchSize.put(partition, currentBatchSize.get(partition) + recordSize);
        }

        @Override
        public void run() {
            startStopLock.lock();
            if (stopped) return; // This happens when the task is still in executor queue
            started = true; // Task is started by executor thread pool
            startStopLock.unlock();

            try {
                for (ConsumerRecord<String, String> record : records) {
                    if (stopped) break;
                    if (readyForCommit(record.timestamp())) {
                        long timeStampInMillis = getBatchTimeStamp();
                        String fileName = commitBatch();
                        String filePath = writeDir + "/" + fileName;
                        String key = getKey(timeStampInMillis, fileName);
                        stageBatchForUpload(filePath, key);
                    }

                    addToBuffer(record);
                    currentOffset.set(record.offset() + 1);
                }
            } catch (IOException e) {
                logger.error("Still pending IOException Handling");
            } catch (InterruptedException e) {
                logger.error("Still pending InterruptedException Handling");
            }

            finished = true;
            completion.complete(currentOffset.get());
        }

        String getKey(long timeStampInMillis, String fileName) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(timeStampInMillis);
            return "topics/" + partition.topic() + "/" + c.get(Calendar.YEAR) + "/" + (c.get(Calendar.MONTH) + 1) + "/"
                    + c.get(Calendar.DAY_OF_MONTH) + "/" + c.get(Calendar.HOUR_OF_DAY) + "/" + fileName;
        }

        void stageBatchForUpload(String filePath, String key) throws InterruptedException {
            uploader.semaphore.acquire();
            uploader.upload(new File(filePath), key);
        }

        public long getCurrentOffset() {
            return currentOffset.get();
        }

        public void stop() {
            startStopLock.lock();
            this.stopped = true;
            if (!started) {
                finished = true;
                completion.complete(currentOffset.get());
            }
            startStopLock.unlock();
        }

        public long waitForCompletion() {
            try {
                return completion.get();
            } catch (InterruptedException | ExecutionException e) {
                return -1;
            }
        }

        public boolean isFinished() {
            return finished;
        }
    }
}
