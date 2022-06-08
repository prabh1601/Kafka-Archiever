package com.prabh.SinkArchiever;

import com.prabh.Utils.AdminController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkApplication {
    private final Logger logger = LoggerFactory.getLogger(SinkApplication.class);
    private final ConsumerClient consumerClient;
    private final WriterClient writerClient;
    private final UploaderClient uploaderClient;
    private final AdminController adminController;

    private SinkApplication(Builder builder) {
        // validate config parameters
        this.adminController = new AdminController(builder.serverId);
        boolean ok = validateConfig(builder);
        if (!ok) {
            logger.error("Application Build Failed");
            throw new RuntimeException();
        }
        // Creating Uploader Client
        this.uploaderClient = new UploaderClient();
        // Creating Writer Client
        this.writerClient = new WriterClient(builder.noOfConsumers, builder.noOfSimultaneousTask, uploaderClient);
        // Creating Consumer Client
        this.consumerClient = new ConsumerClient(writerClient, builder.noOfConsumers, builder.groupName,
                builder.serverId, builder.topic);
    }

    private boolean validateConfig(Builder builder) {
        // Validate Topic
        if (!adminController.exists(builder.topic)) {
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
        uploaderClient.shutdown();
    }

    public static class Builder {
        public String serverId;
        public int noOfConsumers;
        public int noOfSimultaneousTask;
        public String groupName;
        public String topic;

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
            this.noOfSimultaneousTask = _noOfSimultaneousTask;
            return this;
        }

        // Make sure this topic Exists
        public Builder subscribedTopic(String _topic) {
            this.topic = _topic;
            return this;
        }

        public SinkApplication build() {
            return new SinkApplication(this);
        }
    }
}
