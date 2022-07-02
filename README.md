# Kafka/S3 Archiver and Replayer

![](https://img.shields.io/badge/Made%20With-%20java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)

An application that helps to export data from Apache Kafka to Amazon S3 and also replay a certain period of data from s3 back to Kafka

## Features
The use case of this application are twofold
1. Archive data from Kafka clusters to S3 buckets

    - Use Gzip/Snappy for storing data in compressed format
    - multiple concurrent Kafka-Consumers and S3-Uploads
    - Store kafka records in form batches on Amazon S3
    - multiple concurrent batching
    - Exactly once delivery


2. Retrieve data from S3 that belongs in between the input timeperiod and push it to Kafka cluster
    - multiple concurrent Downloads and uploads
    - Resume capabilites for a task
    - Rejection Handler for rejected kafka records

## Getting Started

You can read [this blog]() to get a deep insights on how this application works

## Usage

### AWS Configurations
The AWS user account accessing the S3 bucket must have the following permissions:

- ListAllMyBuckets
- ListBucket
- GetBucketLocation
- PutObject
- GetObject

It is required to user make his own object for S3Client client for security and customization purposes

Refer [here](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html) to get more
insights on how to work with AWS credentials
and [here](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html) for AWS
S3 Client javadocs
<details>
<summary>Example code that uses AWS Keys to instantiate</summary>

`Instantiate S3 Object`

```java
AwsBasicCredentials awsCreds=AwsBasicCredentials.create(
        "your_access_key_id",
        "your_secret_access_key");

S3Client s3=S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
        .region(<region>)
        .build();
```


`Optional Retry Policy`

> It is suggest to overload the default retry policy for better fault tolerance,
> One such example that uses backoff jitter policy can be used from below

```java
// Adding Retry Policy
FullJitterBackoffStrategy backoffStrategy=FullJitterBackoffStrategy.builder()
        .baseDelay(Duration.ofMillis(200))
        .maxBackoffTime(Duration.ofHours(24)).build();

        RetryPolicy policy=RetryPolicy.builder()
        .numRetries(3)
        .additionalRetryConditionsAllowed(true)
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .build();

        ClientOverrideConfiguration conf=ClientOverrideConfiguration.builder()
        .retryPolicy(policy)
        .build();

// Same Code from previous block
AwsBasicCredentials awsCreds=AwsBasicCredentials.create(
        "your_access_key_id",
        "your_secret_access_key");

S3Client s3=S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
        .region(<region>)
        .overrideConfiguration(conf) // Overriding configuration
        .build();
```

</details>

### S3 Sink Archiver

One can use `SinkApplication` API to instantiate the archiver object, which can be instantiated using the
static `SinkApplication.Builder()` builder.

Check below for mandatory and optional parameters inroder to instantiate an object
<details>
<summary> Mandatory Fields </summary>


> The application will not start unless all the Methods mentioned
> are provided. Failure to provide the parameters will lead to `IllegalArgumentException`

| Builder Method  |                    Input Parameters                     |      Parameter Type       | Purpose                                              |
|:---------------:|:-------------------------------------------------------:|:-------------------------:|------------------------------------------------------|
| bootstrapServer |                   Bootstrap Server Id                   |          String           | BootStrap Server Id of the Kafka cluster             |
| subscribeTopics | subscribe Topic<br/> (or)<br/>List of Subscribed Topics | String<br/> (or)<br/>List | Topics to be archived                                |
|    s3Builder    |                  s3Client <br/> bucket                  |    [S3Client](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html)<br/>String    | Amazon S3 Client <br/> Bucket to store archived data |

</details>
<details>
<summary> Optional Fields </summary>

> These options can be used to alter the performance stats for the requirements

| Builder Method  |    Input Parameters    | Parameter Type  |    Default Values    | Purpose                                                       |
|-----------------|:----------------------:|:---------------:|:--------------------:|---------------------------------------------------------------|
| compressionType |    CompressionType     | [CompressionType](https://github.com/prabh1601/Kafka-Archiver/blob/LocalStorageBatching/src/main/java/com/prabh/Utils/CompressionType.java) | CompressionType.NONE | Use `Gzip` or `Snappy` for storing data in compressed format  |
| consumerCount   |     noOfConsumers      |       int       |          3           | No of concurrent consumer clients to be used for consumptions |
| writeTaskCount  | noOfSimultaneousWrites |       int       |          5           | Max no of concurrent Files to be written                      |
| uploadCount     | noOfSimulaneousUploads |       int       |          5           | Max no of concurrent Files to be uploaded                     |
| consumerGroup   |   consumerGroupName    |     String      |    "S3 Archiver"     | Name of the Consumer Group to be used                         |

</details>
<details>
<Summary>Usage Walk-through</Summary>

`1. Instantiate AWS Client`

> Refer AWS Configurations to create AWS Client

`2. Instantiate Sink Application`

* With Mandatory Fields

```java
SinkApplication app=new SinkApplication.Builder()
        .bootstrapServer("BOOTSTRAP-SERVER-ID")
        .subscribedTopics(List.of("testTopic"))
        .s3Builder(s3Client,"BUCKET-NAME")
        .build();
```

* With Optional Fields

> One can use a subset of these fields as per requirements by just adding the builder method to the object build

```java
SinkApplication app=new SinkApplication.Builder()
        .bootstrapServer("BOOTSTRAP-SERVER-ID")
        .subscribedTopics(List.of("testTopic"))
        .s3Builder(s3Client,"BUCKET-NAME")
        .compressionType(CompressionType.GZIP)
        .consumerGroup()
        .build();
```
> [Click Here](https://github.com/prabh1601/Kafka-Archiver/blob/LocalStorageBatching/src/main/java/com/prabh/CodeExamples/ArchiverRun.java) for complete code example
</details>


### S3 Source Replayer</summary>

One can use `SourceApplication` API to instantiate the retrieval object,
which can be instantiated using the static `SourceApplication.Builder()` builder.

Check below sections for mandatory and optional parameters inorder to instantiate an object
<details>
<summary> Mandatory Fields </summary>

> The application will not start unless all the Methods mentioned
> are provided. Failure to provide the parameters will lead to `IllegalArgumentException`

|  Builder Method  |                    Input Parameters                     |         Parameter Type         | Purpose                                                                                                                               |
|:----------------:|:-------------------------------------------------------:|:------------------------------:|---------------------------------------------------------------------------------------------------------------------------------------|
|    s3Builder     |  S3Client <br/> Bucket Name <br/> Retrieve Topic  | [S3Client](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html)<br/>String<br/>String | Amazon S3 Client <br/> Bucket to be used for retrieval<br/> Kafka Topic to be retrieved                                               |
|   kafkaBuilder   |       Bootstrap Server <br/> Produce Topic        |      String<br/> [NewTopic](https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/clients/admin/NewTopic.html)     | Bootstrap Id of the Kafka cluster<br/> Kafka Topic to to be produced <br/>`If topic is not present it creates a new of provided name` |
|      range       |               start epoch <br/> end epoch               |  long (or) FetchRequestRange   | Start of the range to be retrieved<br/>End of the range to be retrieved                                                               |

</details>
<details>
<summary> Optional Fields </summary>

> These options can be used to alter the performance stats for the requirements

| Builder Method      |    Input Parameters     |  Parameter Type  | Default Values | Purpose                                                                |
|---------------------|:-----------------------:|:----------------:|:--------------:|------------------------------------------------------------------------|
| concurrentDownloads | NoOfConcurrentDownloads |       int        |       10       | No of concurrent threads to be used for downloading                    |
| concurrentProducers | NoOfConcurrentProducers |       int        |       7        | No of concurrent threads to be used for producing                      |
| inMemoryStream      |           NA            |        NA        |     false      | Use Heap Memory to download and streaming the content from s3 to kafka |

</details>
<details>
<Summary>Usage Walk-through</Summary>

`1. Instantiate range object`

```java
// Use one of the below for defining start/end

// Using epoch time in millis
long start=1656757978015;
        long end=1656758897543;

// Use FetchRequestRange to put time in calendar format
// Format -> (year, month, day, hour, min) -> Any Valid prefix will work       
        FetchRequestRange start=new FetchRequestRange
        .StartTimestampBuilder(2022,6,28,1,10) // Start from 1:10 on 28/6/2022
        .build();

        FetchRequestRange end=new FetchRequestRange
        .EndTimestampBuilder(2022,7) // End until the end of 7th month of 2022
        .build();
```

`2. Instantiate AWS Client`

> Refer AWS Configurations sections for creating S3Client

`3. Instantiate Source Application`

* With Mandatory Fields

```java
SourceApplication app=new SourceApplication.Builder()
        .s3Builder(s3Client,"BUCKET","CONSUME-TOPIC")
        .kafkaBuilder("BOOTSTRAP-ID",new NewTopic("PRODUCE-TOPIC",<partitions>,(short)1))
        .range(start,end)
        .build();
```

* With Optional Fields

> One can use a subset of these fields as per requirements by just adding the builder method to the object build

```java
SourceApplication app=new SourceApplication.Builder()
        .s3Builder(s3Client,"BUCKET","CONSUME-TOPIC")
        .kafkaBuilder("BOOTSTRAP-ID",new NewTopic("PRODUCE-TOPIC",<partitions>,(short)1))
        .range(start,end)
        .concurrentDownloads(5)
        .concurrentProducers(5)
        .inMemoryStream()
        .build();
```

`4. Start Application`
> Use one of the below options as per requirements

```java
// Overwrites if any previous cache available
app.start();

// Resumes process using any previous local cache available
        app.resume();

// Replays the rejected cache from any previous runs if present
        app.replayRejectedCache();
```

> <b>[Click here](https://github.com/prabh1601/Kafka-Archiver/blob/LocalStorageBatching/src/main/java/com/prabh/CodeExamples/FetcherRun.java)
> for complete code example</b>
</details>

## Contribution

## License
