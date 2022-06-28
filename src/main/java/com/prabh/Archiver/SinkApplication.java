package com.prabh.Archiver;

import com.prabh.Utils.AdminController;
import com.prabh.Utils.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.List;

public class SinkApplication {
    private final Logger logger = LoggerFactory.getLogger(SinkApplication.class);
    private final ConsumerService consumerClient;
    private final WriteService writerClient;
    private final UploadService uploadClient;
    private final AdminController adminController;

    private SinkApplication(Builder builder) {
        // validate config parameters
        this.adminController = new AdminController(builder.serverId);
        boolean ok = validateConfig(builder);
        if (!ok) {
            logger.error("Application Build Failed");
            throw new RuntimeException();
        }

//         Creating Uploader Client
        this.uploadClient = new UploadService(builder.s3Client, builder.bucket, builder.noOfUploads);

//         Creating Writer Client
        this.writerClient = new WriteService(builder.noOfConsumers, builder.noOfSimultaneousWrites, builder.compressionType, uploadClient);

//         Creating Consumer Client
        this.consumerClient = new ConsumerService(writerClient, builder.noOfConsumers, builder.groupName, builder.serverId, builder.subscribedTopics);
    }

    private boolean validateConfig(Builder builder) {
        // Validate Topic
        if (!adminController.exists(builder.subscribedTopics)) {
            logger.error("Build Failed in attempt of subscribing non-existing topic");
            return false;
        }

        // Put Other Validation Checks
        return true;
    }

    public void start() {
        consumerClient.start();
    }

    public void shutdown() {
        consumerClient.shutdown();
        writerClient.shutdown();
        uploadClient.shutdown();
        adminController.shutdown();
    }

    public static class Builder {
        public String serverId;
        public String groupName;
        public List<String> subscribedTopics;
        public int noOfConsumers = 3;
        public int noOfSimultaneousWrites = 5;
        public int noOfUploads = 5;
        public S3Client s3Client;
        public String bucket;
        public CompressionType compressionType = CompressionType.NONE;

        public Builder() {

        }

        // Server to connect
        public Builder bootstrapServer(String _serverId) {
            this.serverId = _serverId;
            return this;
        }

        // Name of Consumer Group for the service
        public Builder consumerGroup(String _groupName) {
            this.groupName = _groupName;
            return this;
        }

        // No of Consumers in the consumer Group
        public Builder consumerCount(int _noOfConsumers) {
            this.noOfConsumers = _noOfConsumers;
            return this;
        }

        // No of threads for ExecutorService
        public Builder writeTaskCount(int _noOfSimultaneousTask) {
            this.noOfSimultaneousWrites = _noOfSimultaneousTask;
            return this;
        }

        // Make sure this topic Exists
        public Builder subscribedTopic(String _topic) {
            this.subscribedTopics = List.of(_topic);
            return this;
        }

        public Builder subscribedTopics(List<String> _topics) {
            this.subscribedTopics = _topics;
            return this;
        }

        // Available options so far : none, Gzip, snappy
        public Builder compressionType(CompressionType _type) {
            this.compressionType = _type;
            return this;
        }

        public Builder s3(S3Client s3Client, String _bucket) {
            this.s3Client = s3Client;
            this.bucket = _bucket;
            return this;
        }

        public SinkApplication build() {
            return new SinkApplication(this);
        }

    }
}
