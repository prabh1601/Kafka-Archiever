package com.prabh.Templates;

import com.prabh.SinkConnector.SinkApplication;
import com.prabh.Utils.AdminController;
import com.prabh.Utils.Config;
import org.apache.kafka.clients.admin.NewTopic;

// Template UseCases
public class Archiver {
    public void SinkConnectorTemplate() {
        // SinkConnector
        SinkApplication app = new SinkApplication.Builder()
                .bootstrapServer(Config.bootstrapServer)
                .consumerGroup(Config.consumerGroup)
                .consumerCount(Config.consumerCount)
                .subscribedTopic(Config.subscribedTopic)
                .writeTaskCount(Config.writeTaskCount)
                .build();

        app.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("Shutdown Hook");
            app.shutdown();
        }));
    }

    public static void main(String[] args) throws InterruptedException {
        new Archiver().SinkConnectorTemplate();
    }
}
