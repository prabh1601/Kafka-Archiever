package com.prabh.Archiver;

import com.prabh.Utils.AdminController;
import com.prabh.Utils.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;

import java.util.List;

public class SinkApplication {
    private final Logger logger = LoggerFactory.getLogger(SinkApplication.class);
    private final ConsumerService consumerClient;
    private final WriteService writerClient;
    private final UploadService uploadClient;
//    private final AdminController adminController;

    private SinkApplication(Builder builder) {
        // validate config parameters

//         Creating Uploader Client
        this.uploadClient = new UploadService(builder.s3Client, builder.bucket, builder.noOfUploads);

//         Creating Writer Client
        this.writerClient = new WriteService(builder.noOfConsumers, builder.noOfSimultaneousWrites, builder.compressionType, uploadClient);

//         Creating Consumer Client
        this.consumerClient = new ConsumerService(writerClient, builder.noOfConsumers, builder.groupName, builder.serverId, builder.subscribedTopics);
    }


    public void start() {
        consumerClient.start();
    }

    public void shutdown() {
        consumerClient.shutdown();
        writerClient.shutdown();
        uploadClient.shutdown();
    }

    public static class Builder {
        public String serverId;
        public String groupName = "S3 Archiver";
        public List<String> subscribedTopics;
        public int noOfConsumers = 3;
        public int noOfSimultaneousWrites = 5;
        public int noOfUploads = 5;
        public S3Client s3Client;
        public String bucket;
        public CompressionType compressionType = CompressionType.NONE;

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
            this.noOfSimultaneousWrites = _noOfSimultaneousTask;
            return this;
        }

        public Builder uploadCount(int _noOfSimultaneousUploads) {
            this.noOfUploads = _noOfSimultaneousUploads;
            return this;
        }

        // Make sure this topic Exists
        public Builder subscribedTopic(String _topic) {
            this.subscribedTopics = List.of(_topic);
            return this;
        }

        public Builder subscribedTopics(List<String> _topics) {
            this.subscribedTopics = _topics;
            return this;
        }

        // Available options so far : none, Gzip, snappy
        public Builder compressionType(CompressionType _type) {
            this.compressionType = _type;
            return this;
        }

        public Builder s3Builder(S3Client s3Client, String _bucket) {
            this.s3Client = s3Client;
            this.bucket = _bucket;
            return this;
        }

        public void validate() {
            if (serverId == null) {
                throw new IllegalArgumentException("Bootstrap Server Id cannot be null");
            }
            if (subscribedTopics == null) {
                throw new IllegalArgumentException("Subscribed Topics cannot be null");
            } else {
                AdminController adminController = new AdminController(serverId);
                if (!adminController.exists(subscribedTopics)) {
                    throw new IllegalArgumentException("Attempt to subscribe non-existing topic");
                }
                adminController.shutdown();
            }

            if (s3Client == null) {
                throw new IllegalArgumentException("S3Client cannot be null");
            } else if (bucket == null) {
                throw new IllegalArgumentException("Destination Bucket cannot be null");
            } else {
                GetBucketAclRequest request = GetBucketAclRequest.builder().bucket(bucket).build();
                try {
                    s3Client.getBucketAcl(request);
                } catch (AwsServiceException ase) {
                    if (ase.statusCode() == 404) {
                        throw new IllegalArgumentException("Destination Bucket Doesnt Exist");
                    } else if (ase.statusCode() == 301) {
                        throw new IllegalArgumentException("Defined S3 Region doesnt match the bucket configurations");
                    }
                } catch (SdkClientException e) {
                    throw new IllegalArgumentException(e.getMessage());
                }
            }
        }

        public SinkApplication build() {
            validate();
            return new SinkApplication(this);
        }
    }
}
