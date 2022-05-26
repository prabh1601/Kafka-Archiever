package com.prabh.Utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminController {
    private final Logger logger = LoggerFactory.getLogger(AdminController.class);
    private Admin client;

    private AdminController(Builder builder) {
        Properties prop = new Properties();
        prop.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, builder._BOOTSTRAP_ID);
        this.client = Admin.create(prop);
    }

    public static class Builder {
        private String _BOOTSTRAP_ID;

        public Builder() {

        }

        public Builder bootstrapId(String id) {
            this._BOOTSTRAP_ID = id;
            return this;
        }

        public AdminController build() {
            return new AdminController(this);
        }
    }

    List<String> getTopics() {
        try {
            return client.listTopics().names().get().stream().toList();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("Something went wrong while retrieving topics");
            return null;
        }
    }
}
