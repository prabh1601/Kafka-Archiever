package com.prabh.Utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminController {
    private final Logger logger = LoggerFactory.getLogger(AdminController.class);
    private final Admin client;

    public AdminController(String serverId) {
        Properties prop = new Properties();
        prop.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, serverId);
        this.client = Admin.create(prop);
    }

    public List<String> getTopics() {
        try {
            return client.listTopics().names().get().stream().toList();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Topic List Retrieval failed");
            return null;
        }
    }

    public void shutdown() {
        logger.warn("Admin Controller Shutting down");
        client.close();
    }

    public boolean create(NewTopic topic) {
        if (exists(topic.name())) {
            logger.warn("Asked Topic-{} already exists, skipping creation", topic.name());
            return true;
        }

        logger.warn("Creating topic {}", topic);
        CreateTopicsResult result = client.createTopics(List.of(topic));
        try {
            result.all().get();
            return true;
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    public boolean exists(String topic) {
        try {
            return client.listTopics().names().get().contains(topic);
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Topic Check failed");
            return false;
        }
    }
}
