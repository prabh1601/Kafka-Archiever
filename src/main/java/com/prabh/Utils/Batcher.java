package com.prabh.Utils;

import com.prabh.SinkArchiever.ConsumerClient;

import java.io.File;

public class Batcher implements Runnable {
    ConsumerClient runningClient;
    AwsClient s3client = new AwsClient();

    public Batcher(ConsumerClient client) {
        this.runningClient = client;
    }

    public void run() {
        runningClient.shutdown();
        String oldDirPath = "/mnt/Drive1/Write";
        String newDirPath = "/mnt/Drive1/Upload";
        File oldDir = new File(oldDirPath);
        File newDir = new File(newDirPath);
        oldDir.renameTo(newDir);
        oldDir.mkdir();
        File[] files = newDir.listFiles();
        for (File file : files) {
            file.delete();
        }
        runningClient.start();
//        s3client.uploadBatch(newDirPath); -> Still need to implement this
        newDir.delete();
    }
}
