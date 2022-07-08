package com.prabh.Fetcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ProgressListener {
    private final Logger logger = LoggerFactory.getLogger(ProgressListener.class);
    private static final ScheduledExecutorService progressListenerExecutor = Executors.newScheduledThreadPool(1, r -> new Thread(r, "Progress Listener"));
    private final ConcurrentHashMap<String, Boolean> downloadedObjects = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> producedObjects = new ConcurrentHashMap<>();
    private final FilePaths filePaths;
    private final AtomicLong totalProcessedRecords = new AtomicLong(0);
    private final AtomicLong rejectedRecords = new AtomicLong(0);
    private final long totalFiles;
    private final AtomicLong producedFiles;
    private final AtomicLong downloadedFiles;

    public ProgressListener(FilePaths _filePaths, long total, int alreadyProduced, int alreadyDownloaded) {
        this.filePaths = _filePaths;
        this.totalFiles = total;
        this.producedFiles = new AtomicLong(alreadyProduced);
        this.downloadedFiles = new AtomicLong(alreadyDownloaded);
    }

    public void stop() {
        try {
            Thread.sleep(2000);
            progressListenerExecutor.shutdown();
            progressListenerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void start(final long time, final TimeUnit timeUnit) {
        Runnable run = new Runnable() {
            public double getPercentage(long a, long b) {
                long progress = (long) (((double) a / b) * 10000);
                return (double) progress / 100;
            }

            public void logProgress() {
                long downloaded = downloadedFiles.get();
                long produced = producedFiles.get();
                double downloadProgress = getPercentage(downloaded, totalFiles);
                double producedProgress = getPercentage(produced, totalFiles);
                long totalRecords = totalProcessedRecords.get();
                long rejected = rejectedRecords.get();
                double rejectionRate = totalRecords == 0 ? 0 : getPercentage(rejected, totalRecords);
                logger.warn("Downloaded Files : {} % ({} of {}) | Produced Files : {} % ({} of {}) | Total Processed Records : {} | Rejection Rate : {} % - ({} of {}) ",
                        downloadProgress, downloaded, totalFiles, producedProgress, produced, totalFiles,
                        totalRecords, rejectionRate, rejected, totalRecords);
            }

            public void write(List<String> objects, ConcurrentHashMap<String, Boolean> status, String filePath) {
                try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))) {
                    for (String s : objects) {
                        writer.println(s);
                        status.remove(s);
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

            public void commitLocalStatus() {
                List<String> downloadedObjectsPendingCommit = new ArrayList<>(downloadedObjects.keySet());
                List<String> committedProducedObjects = new ArrayList<>(producedObjects.keySet());
                write(downloadedObjectsPendingCommit, downloadedObjects, filePaths.DownloadedObjectListFile);
                write(committedProducedObjects, producedObjects, filePaths.ProducedObjectListFile);
            }

            @Override
            public void run() {
                commitLocalStatus();
                logProgress();
            }
        };

        progressListenerExecutor.scheduleWithFixedDelay(run, time, time, timeUnit);
    }

    public void markDownloadedObject(String objectKey) {
        downloadedObjects.put(objectKey, true);
        downloadedFiles.incrementAndGet();
    }

    public void markProducedObject(String objectKey) {
        producedObjects.put(objectKey, true);
        if (!objectKey.equals(FilePaths.POISON_PILL)) {
            producedFiles.incrementAndGet();
        }
    }

    public void markRejectedRecord() {
        rejectedRecords.incrementAndGet();
    }

    public void markProducedRecord() {
        totalProcessedRecords.incrementAndGet();
    }

}
