package com.prabh.SourceConnector;

import com.prabh.Utils.AdminController;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceApplication {
    private final Logger logger = LoggerFactory.getLogger(SourceApplication.class);
    private final DownloadingService downloadingService;
    private final ProducerService producerService;
    private final AdminController adminController;
    //    private final ProcessingService processingService;

    private SourceApplication(Builder builder) {
        this.adminController = new AdminController(builder.bootstrapId);
        adminController.create(builder.produceTopic);
        this.producerService = new ProducerService(builder.produceTopic.name(), builder.bootstrapId);
        this.downloadingService = new DownloadingService(builder.consumeTopic, builder.startStamp, builder.endStamp,
                this.producerService);
    }

    public void start() {
        new Thread(downloadingService).start();
    }

    public void shutdown() {
        logger.warn("Source Application Shutdown Initiated");
        adminController.shutdown();
    }

    // check if cache for consumeTopic exists
    public static class Builder {
        private String bootstrapId;
        private RequestObject startStamp;
        private RequestObject endStamp;
        private String consumeTopic;
        private NewTopic produceTopic;

        public Builder() {

        }

        public Builder bootstrapServer(String _bootstrapId) {
            this.bootstrapId = _bootstrapId;
            return this;
        }

        public Builder consumeTopic(String topic) {
            this.consumeTopic = topic;
            return this;
        }

//        public Builder produceTopic(String topic) {
//            this.produceTopic = topic;
//            return this;
//        }

        public Builder produceTopic(NewTopic topic) {
            this.produceTopic = topic;
            return this;
        }

        public Builder range(RequestObject _startStamp, RequestObject _endStamp) {
            this.startStamp = _startStamp;
            this.endStamp = _endStamp;
            return this;
        }

        public SourceApplication build() {
            return new SourceApplication(this);
        }
    }
}
