package com.prabh.Fetcher;

import com.prabh.Utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProgressListener {
    private final Logger logger = LoggerFactory.getLogger(ProgressListener.class);
    private static final AtomicInteger THREAD_COUNT = new AtomicInteger();
    private static final ScheduledExecutorService progressListenerExecutor = Executors.newScheduledThreadPool(4, r -> {
        final Thread thread = new Thread(r, "Progress Listener - " + THREAD_COUNT.incrementAndGet());
        return thread;
    });
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, Boolean> downloadedObjects = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> producedObjects = new ConcurrentHashMap<>();
    private final FilePaths filePaths;

    public ProgressListener(FilePaths _filePaths) {
        this.filePaths = _filePaths;
    }

    public void start(final long time, final TimeUnit timeUnit) {
        Runnable updateObject = new Runnable() {

            public boolean write(List<String> objects, ConcurrentHashMap<String, Boolean> status, String filePath) {

                boolean endCommunication = false;
                try (PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(filePath, true)))) {
                    for (String s : objects) {
                        if (s.equals(FilePaths.POISON_PILL)) {
                            endCommunication = true;
                        } else {
                            writer.println(s);
                            status.remove(s);
                        }
                    }
                    return endCommunication;
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    return false;
                }
            }

            public boolean commitLocalStatus() {
                List<String> downloadedObjectsPendingCommit = new ArrayList<>(downloadedObjects.keySet());
                List<String> committedProducedObjects = new ArrayList<>(producedObjects.keySet());
                write(downloadedObjectsPendingCommit, downloadedObjects, filePaths.DownloadedObjectListFile);
                return write(committedProducedObjects, producedObjects, filePaths.ProducedObjectListFile);
            }

            @Override
            public void run() {
                boolean endCommunication = commitLocalStatus();
//                logProgress();

                if(endCommunication) System.out.println("lol");
                if (!endCommunication) {
                    progressListenerExecutor.schedule(this, time, timeUnit);
                } else {
                    progressListenerExecutor.shutdown();
                }
            }
        };

        progressListenerExecutor.schedule(updateObject, time, timeUnit);
    }

    public void stop() {
        stopped.set(true);
    }

    public void markDownloadedObject(String objectKey) {
        downloadedObjects.put(objectKey, true);
    }

    public void markProducedObject(String objectKey) {
        producedObjects.put(objectKey, true);
    }

}
