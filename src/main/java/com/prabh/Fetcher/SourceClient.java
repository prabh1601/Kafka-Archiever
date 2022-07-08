package com.prabh.Fetcher;

import com.prabh.Utils.AdminController;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;

import java.util.concurrent.CountDownLatch;

public class SourceClient {
    private final Logger logger = LoggerFactory.getLogger(SourceClient.class);
    private final DownloadService downloadingService;
    private final ProducerService producerService;
    private final CountDownLatch completion = new CountDownLatch(1);

    private SourceClient(Builder builder) {
        FilePaths filePaths = new FilePaths(builder.startStamp.getStamp(), builder.endStamp.getStamp());
        this.producerService = new ProducerService(builder.produceTopic.name(), builder.bootstrapId, filePaths,
                completion, builder.producerThreadCount);
        this.downloadingService = new DownloadService(builder.s3Client, builder.bucket, builder.consumeTopic,
                builder.startStamp, builder.endStamp, producerService, builder.stream, filePaths,
                builder.downloadThreadCount);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Thread.currentThread().setName("Shutdown Hook");
            shutdown();
        }));
    }

    // Overwrites the local cached progress if present
    public void start() {
        downloadingService.start(false);
    }

    // Uses the local cached progress
    public void resume() {
        downloadingService.start(true);
    }

    // Can try replaying rejected Records once again
    public void replayRejectedCache() {
        downloadingService.replayRejectedCache();
    }

    public void shutdown() {
        downloadingService.forceShutdown();
        try {
            completion.await();
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
        logger.warn("Application Shutdown Complete");
    }

    public static class Builder {
        private FetchRequestRange startStamp;
        private FetchRequestRange endStamp;
        private S3Client s3Client;
        private String consumeTopic;
        private String bucket;
        private String bootstrapId;
        private NewTopic produceTopic;
        private int downloadThreadCount = 20;
        private int producerThreadCount = 5;
        private boolean stream = false;

        public Builder() {

        }


//        public Builder bucket(String _bucket) {
//            this.bucket = _bucket;
//            return this;
//        }
//
//        public Builder s3Client(S3Client _s3Client) {
//            this.s3Client = _s3Client;
//            return this;
//        }
//        public Builder consumeTopic(String topic) {
//            this.consumeTopic = topic;
//            return this;
//        }
//        public Builder bootstrapServer(String _bootstrapId) {
//            this.bootstrapId = _bootstrapId;
//            return this;
//        }
//
//        public Builder produceTopic(NewTopic topic) {
//            this.produceTopic = topic;
//            return this;
//        }

        public Builder s3Builder(S3Client s3Client, String bucket, String topic) {
            this.s3Client = s3Client;
            this.bucket = bucket;
            this.consumeTopic = topic;
            return this;
        }

        public Builder kafkaBuilder(String BootstrapServerId, NewTopic topic) {
            this.bootstrapId = BootstrapServerId;
            this.produceTopic = topic;
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
            } else if (startStamp == null || endStamp == null)
                throw new IllegalArgumentException("Missing or Invalid queried epoch range");
            if (produceTopic == null) {
                throw new IllegalArgumentException("Parameter 'Produce Topic' must not be null");
            } else {
                // Helps checking kafka connections before start
                AdminController adminController = new AdminController(bootstrapId);
                adminController.create(produceTopic);
                adminController.shutdown();

                // Check if bucket and aws client are ok
                GetBucketAclRequest request = GetBucketAclRequest.builder().bucket(bucket).build();
                try {
                    s3Client.getBucketAcl(request);
                } catch (AwsServiceException ase) {
                    if (ase.statusCode() == 404) {
                        throw new IllegalArgumentException("Destination Bucket doesn't exist");
                    } else if (ase.statusCode() == 301) {
                        throw new IllegalArgumentException("Defined S3 Region doesn't match the bucket configurations");
                    }
                } catch (SdkClientException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        public SourceClient build() {
            validate();
            return new SourceClient(this);
        }
    }
}
