package com.prabh.SinkArchiever;

import com.amazonaws.services.customerprofiles.model.Batch;
import com.prabh.Utils.AwsClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.concurrent.*;

import com.prabh.Utils.Task;

public class Collector {
    private final Logger logger = LoggerFactory.getLogger(Collector.class.getName());
    private final ExecutorService taskExecutor;
    private CountDownLatch canWrite;
    private final ScheduledExecutorService batchUploader;


    public Collector(int taskPoolSize) {
        taskExecutor = Executors.newFixedThreadPool(taskPoolSize);
        batchUploader = Executors.newScheduledThreadPool(1);
        batchUploader.scheduleAtFixedRate(new Batch(), 10, 30, TimeUnit.SECONDS);
        pauseWriting();
        resumeWriting();
    }

    public Task submit(List<ConsumerRecord<String, String>> records) {
        Task t = new Task(records);
        try {
            canWrite.await();
        } catch (InterruptedException e) {
            logger.error("Batcher Await Interrupted");
        }
        taskExecutor.submit(t);
        return t;
    }

    public void stop() {
        logger.warn("Task Executor Shutting down");
        taskExecutor.shutdown();
    }

    public void pauseWriting() {
        logger.info("Pausing the Writing Tasks");
        canWrite = new CountDownLatch(1);
    }

    public void resumeWriting() {
        logger.info("Resuming the Writing Tasks");
        canWrite.countDown();
    }

    private class Batch implements Runnable {
        AwsClient s3Client = new AwsClient();

        public Batch() {
        }

        public void run() {
            logger.info("Periodic Batch Upload scheduled");
            // 1. stop further tasks to be started
            pauseWriting();
            // 2. wait till on ongoing writing tasks are finished (Another option : Maybe just kill them ?)

            // 3. Change the name of current folder
            File dir = new File("/mnt/Drive1/Write");
            File newDir = new File(dir.getParent() + "/Upload");
            dir.renameTo(newDir);
            // 4. Make new folder of previous name
            dir.mkdirs();
            // 5. resume task loading
            resumeWriting();
            logger.info("Starting Batch uploading");
            // 6. Upload folder
            s3Client.uploadBatch();
            // 7. Delete upload folder
            newDir.delete();
        }
    }
}
