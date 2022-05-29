package com.prabh.SinkArchiever;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerClient {
    private final Logger logger = LoggerFactory.getLogger(ConsumerClient.class);
    private final List<ConsumerThread> consumers;
    private Builder builder;

    private ConsumerClient(Builder _builder) {
        this.builder = _builder;
        consumers = new ArrayList<>(builder.noOfConsumers);
        for (int i = 0; i < builder.noOfConsumers; i++) {
            ConsumerThread c = new ConsumerThread(_builder);
            c.start();
            synchronized (consumers) {
                consumers.add(c);
            }
        }
    }

    public static class Builder {
        public String serverId;
        public int noOfConsumers;
        public int noOfSimultaneousTask;
        public String groupName;
        public String topic;

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
        public Builder taskCount(int _noOfSimultaneousTask) {
            this.noOfSimultaneousTask = _noOfSimultaneousTask;
            return this;
        }

        public Builder subscribedTopic(String _topic) {
            this.topic = _topic;
            return this;
        }

        public ConsumerClient build() {
            return new ConsumerClient(this);
        }

    }

}
