package com.prabh.Templates;

import com.prabh.SourceConnector.RequestObject;
import com.prabh.SourceConnector.SourceApplication;
import org.apache.kafka.clients.admin.NewTopic;

public class Fetcher {
    public void SourceConnectorTemplate() {
        // Source Connector
        // Constructor : (year, month, date, hour, min) -> Not required to put all 5, any prefix of this values work
        RequestObject start = new RequestObject
                .StartTimestampBuilder(2022, 6, 10, 10, 50)
                .build();

        RequestObject end = new RequestObject
                .EndTimestampBuilder(2022, 6, 10, 10, 55)
                .build();

        SourceApplication app = new SourceApplication.Builder()
                .bootstrapServer("localhost:9092")
                .consumeTopic("test")
                .produceTopic(new NewTopic("produceTest", 4, (short) 1))
                .range(start, end)
                .build();

        app.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("Shutdown Hook");
            app.shutdown();
        }));
    }

    public static void main(String[] args) {
        new Fetcher().SourceConnectorTemplate();
    }
}
