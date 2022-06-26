package com.prabh.Fetcher;

import com.prabh.Utils.AdminController;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Properties;

public class SourceApplication {
    private final Logger logger = LoggerFactory.getLogger(SourceApplication.class);
    private final DownloadService downloadingService;
    private final ProducerService producerService;
    private final AdminController adminController;
    private final NewTopic produceTopic;

    private SourceApplication(Builder builder) {
        this.produceTopic = builder.produceTopic;
        String localDumpLocation = String.format("%s/S3ToKafkaReplay/%d", System.getProperty("java.io.tmpdir"),
                System.currentTimeMillis());
        this.adminController = new AdminController(builder.bootstrapId);
        this.producerService = new ProducerService(builder.produceTopic.name(), builder.bootstrapId, localDumpLocation,
                builder.producerThreadCount);
        this.downloadingService = new DownloadService(builder.s3Client, builder.bucket, builder.consumeTopic,
                builder.startStamp, builder.endStamp, producerService, builder.stream, localDumpLocation,
                builder.downloadThreadCount);
    }

    public void start() {
        // Check if
        boolean ok = adminController.create(produceTopic);
        if (!ok) {
            logger.error("Application Start failed");
            return;
        }
        downloadingService.run();
    }

    public void shutdown() {
        logger.warn("Source Application Shutdown Initiated");
        adminController.shutdown();
    }

    // check if cache for consumeTopic exists
    public static class Builder {
        private FetchRequestRange startStamp;
        private FetchRequestRange endStamp;
        private S3Client s3Client;
        private String consumeTopic;
        private String bucket;
        private String bootstrapId;
        private NewTopic produceTopic;
        private int producerThreadCount = 5;
        private int downloadThreadCount = 5;
        private boolean stream = false;

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

        public Builder produceTopic(NewTopic topic) {
            this.produceTopic = topic;
            return this;
        }

        public Builder bucket(String _bucket) {
            this.bucket = _bucket;
            return this;
        }

        public Builder s3Client(S3Client _s3Client) {
            this.s3Client = _s3Client;
            return this;
        }

        public Builder range(FetchRequestRange from, FetchRequestRange to) {
            this.startStamp = from;
            this.endStamp = to;
            return this;
        }

        public Builder range(long from, long to) {
            this.startStamp = new FetchRequestRange.StartTimestampBuilder(from).build();
            this.endStamp = new FetchRequestRange.EndTimestampBuilder(to).build();
            return this;
        }

        public Builder concurrentDownloads(int count) {
            this.downloadThreadCount = count;
            return this;
        }

        public Builder concurrentProducers(int count) {
            this.producerThreadCount = count;
            return this;
        }

        public Builder inMemoryStream() {
            this.stream = true;
            return this;
        }

        private void validate() {
            if (bootstrapId == null) {
                throw new IllegalArgumentException("Parameter 'Kafka Broker Bootstrap Id' must not be null");
            } else if (consumeTopic == null) {
                throw new IllegalArgumentException("Parameter 'Consume Topic' must not be null");
            } else if (s3Client == null) {
                throw new IllegalArgumentException("Parameter 'S3 Client' must not be null");
            } else if (bucket == null) {
                throw new IllegalArgumentException("Parameter 'Bucket' must not be null");
            } else if (produceTopic == null) {
                throw new IllegalArgumentException("Parameter 'Produce Topic' must not be null");
            } else if (startStamp == null || endStamp == null)
                throw new IllegalArgumentException("Missing or Invalid queried epoch range");
        }

        public SourceApplication build() {
            validate();
            return new SourceApplication(this);
        }
    }
}
