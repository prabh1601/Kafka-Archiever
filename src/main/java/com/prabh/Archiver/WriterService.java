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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class WriterService {
    private final Logger logger = LoggerFactory.getLogger(WriterService.class);
    private final ExecutorService taskExecutor;
    private final List<ConcurrentHashMap<TopicPartition, WritingTask>> activeTasks;
    private final UploaderService uploader;
    private final CompressionType compressionType;

    WriterService(int noOfConsumers, int taskPoolSize, String _compressionType, UploaderService _uploader) {
        this.uploader = _uploader;
        this.compressionType = CompressionType.getCompressionType(_compressionType);
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
        private final ZonedDateTime zdt = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"));
        private final int chunkSize = 10;
        private final String fileName;

        public WritingTask(List<ConsumerRecord<String, String>> _records, TopicPartition _partition) {
            this.records = _records;
            this.partition = _partition;
            String writeDir = Config.WriterServiceDir + "/" + partition.topic();
            new File(writeDir).mkdirs();
            this.fileName = writeDir + "/write-" + partition.partition() + ".txt";
        }

        boolean readyForUpload() throws IOException {
            long fileSizeInMB = Files.size(Paths.get(fileName)) / (1024 * 1024);
            if (fileSizeInMB > chunkSize) return true;

            return false;
        }

        PrintWriter getWriter() throws IOException {
            return new PrintWriter(
                    compressionType.wrapCompressionStream(
                            new BufferedOutputStream(
                                    new FileOutputStream(fileName, true))));
        }

        @Override
        public void run() {
            startStopLock.lock();
            if (stopped) return; // This happens when the task is still in executor queue
            started = true; // Task is started by executor thread pool
            startStopLock.unlock();

            try {
                PrintWriter writer = getWriter();
                for (ConsumerRecord<String, String> record : records) {
                    if (stopped) break;
                    writer.println(record.value());
                    currentOffset.set(record.offset() + 1);
                }

                writer.close();

                if (readyForUpload()) {
                    uploadAndRotateShift();
                }
            } catch (IOException e) {
                logger.error("IOException while writing into local files");
            }

            finished = true;
            completion.complete(currentOffset.get());
        }

        String getKey(TopicPartition partition) {
            return "topics/" + partition.topic() + "/" + zdt.getYear() + "/" + zdt.getMonthValue() + "/"
                    + zdt.getDayOfMonth() + "/" + zdt.getHour() + "/" + zdt.getMinute() + "_"
                    + zdt.getSecond() + "_" + partition.partition();
        }

        void uploadAndRotateShift() {
            try {
                uploader.semaphore.acquire();
            } catch (Exception e) {
                logger.error("{} failed to acquire semaphore", fileName);
            }
            File f_old = new File(fileName);
            File f_new = new File(f_old.getParent() + "/upload-" + zdt.getMinute() + "-" + zdt.getSecond() + "-" + partition.partition() + ".txt");
            boolean renameStatus = f_old.renameTo(f_new);
            if (!renameStatus) {
                logger.error("{} failed to stage for upload due to renaming failure", f_old.getName());
            }
            uploader.upload(f_new, getKey(partition));
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
