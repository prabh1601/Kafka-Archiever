package com.prabh.Utils;

import com.prabh.SinkConnector.SinkApplication;
import com.prabh.SourceConnector.DownloadingService;
import com.prabh.SourceConnector.RequestObject;

// Template UseCases
public class Main {
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

    public void SourceConnectorTemplate() {
        // Source Connector
        // Constructor : (year, month, date, hour, min) -> Not required to put all 5, any prefix of this values work
        RequestObject start = new RequestObject
                .StartTimestampBuilder(2022, 6, 10, 12, 5)
                .build();

        RequestObject end = new RequestObject
                .EndTimestampBuilder(2022, 6, 10)
                .build();

        DownloadingService d = new DownloadingService("test", start, end);
        d.run();
    }

    public static void main(String[] args) throws InterruptedException {
//        new Main().SinkConnectorTemplate();
//        new Main().SourceConnectorTemplate();
    }
}
