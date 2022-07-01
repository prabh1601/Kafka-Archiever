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
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, Boolean> downloadedObjects = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> producedObjects = new ConcurrentHashMap<>();
    private final FilePaths filePaths;
    private final long totalFiles;
    private volatile long startTime;
    private volatile long lastIterStartTime;
    private final ProgressCalculator downloadStatus;
    private final ProgressCalculator produceStatus;
    private final ProgressCalculator rejectionStatus;

    public ProgressListener(FilePaths _filePaths, long downloadedFiles, long totalFiles) {
        this.filePaths = _filePaths;
        this.totalFiles = totalFiles;
        this.downloadStatus = new ProgressCalculator(downloadedFiles);
        this.produceStatus = new ProgressCalculator(downloadedFiles);
        this.rejectionStatus = new ProgressCalculator(0);
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

    public void markFailedRecord() {
        rejectionStatus.incrementOpCount();
    }

    public void markDownloadedObject(String objectKey) {
        downloadedObjects.put(objectKey, true);
        downloadStatus.incrementOpCount();
    }

    public void markProducedObject(String objectKey) {
        producedObjects.put(objectKey, true);
        produceStatus.incrementOpCount();
    }

    private class ProgressCalculator {
        private final AtomicLong currentIterationOpsCount;
        private final AtomicLong totalOpsCount;

        public ProgressCalculator(long ops) {
            this.totalOpsCount = new AtomicLong(ops);
            this.currentIterationOpsCount = new AtomicLong(ops);
        }

        public void incrementOpCount() {
            totalOpsCount.incrementAndGet();
            currentIterationOpsCount.incrementAndGet();
        }

        public void incrementOpCount(int delta) {
            totalOpsCount.addAndGet(delta);
            currentIterationOpsCount.addAndGet(delta);
        }

        public long getTotalOpsCount() {
            return totalOpsCount.get();
        }

        public Pair<Pair<Long, Long>, Pair<Long, Long>> stat() {
            final long now = System.nanoTime();
            long totalOps = totalOpsCount.get();
            long currentIterCount = currentIterationOpsCount.getAndSet(0);

            long totalElapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - startTime);
            long iterationElapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - lastIterStartTime);

            return new Pair<>(new Pair<>(totalOps, currentIterCount),
                    new Pair<>(totalElapsedMillis, iterationElapsedMillis));
        }

        public ProgressStat getTPSStat() {
            final long now = System.nanoTime();
            long totalOps = totalOpsCount.get();
            long currentIterCount = currentIterationOpsCount.getAndSet(0);

            long totalElapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - startTime);
            long iterationElapsedMillis = TimeUnit.NANOSECONDS.toMillis(now - lastIterStartTime);
            return new ProgressStat(totalOps, currentIterCount, totalElapsedMillis, iterationElapsedMillis);
        }

        public static String getTps(long totalElapsedMillis, long totalOpsCount) {
            long seconds = totalElapsedMillis / 1000;
            if (seconds <= 0) {
                return String.valueOf(totalOpsCount);
            }
            return String.valueOf(totalOpsCount / seconds);
        }

        private class ProgressStat {
            private final long totalOpsCount;
            private final long totalElapsedMillis;
            private final String totalTps;
            private final String iterationTps;

            public ProgressStat(long totalOpsCount, long iterationOpsCount, long totalElapsedMillis, long iterationElapsedMillis) {
                this.totalOpsCount = totalOpsCount;
                this.totalElapsedMillis = totalElapsedMillis;
                totalTps = getTps(totalElapsedMillis, totalOpsCount);
                iterationTps = getTps(iterationElapsedMillis, iterationOpsCount);
            }

//            public String getProgress() {
//            }
        }

        public String getHumanReadableTotalTime() {
            StringBuilder sb = new StringBuilder();
            long now = System.nanoTime();
            long totalElapsedSeconds = TimeUnit.NANOSECONDS.toSeconds(now - startTime);
            long seconds = startTime / 1000000000;
            long days = seconds / (3600 * 24);
            append(sb, days, "d");
            seconds -= (days * 3600 * 24);
            long hours = seconds / 3600;
            append(sb, hours, "h");
            seconds -= (hours * 3600);
            long minutes = seconds / 60;
            append(sb, minutes, "m");
            seconds -= (minutes * 60);
            append(sb, seconds, "s");
            return sb.toString();
        }

        public void append(StringBuilder sb, long value, String text) {
            if (value > 0) {
                if (sb.length() > 0) {
                    sb.append(" ");
                }
                sb.append(value).append(text);
            }
        }
    }
}
