# Kafka to S3 Archiver and Replayer
![](https://img.shields.io/badge/Made%20With-%20java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)

Kafka stores data in partitions with a specific retention period, it might happen that at some point, 
replaying data between a specific timeperiod is required. This application incorporates this tasks in
two independant components.

The usecase of this application are two fold
1. Archive data from Kafka clusters to S3 buckets
2. Retrieve data from S3 that belongs in between the input timeperiod and push it to Kafka cluster

You can try reading [this blog]() to get a deep insights on how this application works


## Getting Started
<Details>
<summary>AWS Configurations</summary>

Inorder to give user a customizable experience, it is enabled to get user make his own object for S3Client client.
* Please Refer [here](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html) to get more insights on how to work with credentials and [here](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3Client.html) for AWS s3client javadocs
* Below Example uses AWS Keys to instantiate

`Instantiate S3 Object`
```java
AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
        "your_access_key_id",
        "your_secret_access_key");
    
S3Client s3 = S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
        .region(<region>)
        .build();
```

`Optional Retry Policy`

> Default retry policy can be overloaded for better fault tolerance,
> One such example that uses backoff jitter policy can be used from below
```java
// Adding Retry Policy
FullJitterBackoffStrategy backoffStrategy = FullJitterBackoffStrategy.builder()
        .baseDelay(Duration.ofMillis(200))
        .maxBackoffTime(Duration.ofHours(24)).build();

RetryPolicy policy = RetryPolicy.builder()
        .numRetries(3)
        .additionalRetryConditionsAllowed(true)
        .backoffStrategy(backoffStrategy)
        .throttlingBackoffStrategy(backoffStrategy)
        .build();

ClientOverrideConfiguration conf = ClientOverrideConfiguration.builder()
        .retryPolicy(policy)
        .build();

// Same Code from previous block
AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
        "your_access_key_id",
        "your_secret_access_key");

S3Client s3 = S3Client.builder()
        .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
        .region(<region>)
        .overrideConfiguration(conf) // Overriding configuration
        .build();
```
</Details>

<Details>
<summary>S3 Sink Archiver</summary>

</Details>
<Details>
<summary>S3 Source Replayer</summary>

In order to use the application, one can use `SourceApplication` API,
which can be instantiated using the static `SourceApplication.Builder()` method.

Check below sections for mandatory and optional parameters inorder to instantiate an object
<details>
<summary>
<ins><b>Mandatory Fields</b></ins>
</summary>

> The application will not start unless all the Methods mentioned
are provided. Failure to provide the parameters will lead to `IllegalArgumentException`

|  Builder Method  |                    Input Parameters                     |         Parameter Type         | Purpose                                                                                                                               |
|:----------------:|:-------------------------------------------------------:|:------------------------------:|---------------------------------------------------------------------------------------------------------------------------------------|
|    s3Builder     |  S3Client <br/> Bucket Name <br/> Kafka Retrieve Topic  | S3Client<br/>String<br/>String | Amazon S3 Client <br/> Bucket to be used for retrieval<br/> Kafka Topic to be retrieved                                               |
|   kafkaBuilder   |       Bootstrap Server <br/> Kafka Produce Topic        |      String<br/> NewTopic      | Bootstrap Id of the Kafka cluster<br/> Kafka Topic to to be produced <br/>`If topic is not present it creates a new of provided name` |
|      range       |               start epoch <br/> end epoch               |  long (or) FetchRequestRange   | Start of the range to be retrieved<br/>End of the range to be retrieved                                                               |
</details>
<details>
<summary>
<ins><b>Optional Fields</b></ins>
</summary>

> These options can be used to alter the performance stats for the requirements

| Builder Method      |    Input Parameters     |  Parameter Type  | Purpose                                                                |
|---------------------|:-----------------------:|:----------------:|------------------------------------------------------------------------|
| concurrentDownloads | NoOfConcurrentDownloads |       int        | No of concurrent threads to be used for downloading                    |
| concurrentProducers | NoOfConcurrentProducers |       int        | No of concurrent threads to be used for producing                      |
| inMemoryStream      |           NA            |        NA        | Use Heap Memory to download and streaming the content from s3 to kafka |

</details>
<details>
<Summary><b><ins>Code Walkthrough</ins></b></Summary>

`1. Instantiate range object`
```java
// Use one of the below for defining start/end

// Using epoch time in millis
long start = 1656757978015;
long end = 1656758897543;

// Use FetchRequestRange to put time in calendar format
// Format -> (year, month, day, hour, min) -> Any Valid prefix will work       
 FetchRequestRange start = new FetchRequestRange
        .StartTimestampBuilder(2022, 6, 28, 1, 10) // Start from 1:10 on 28/6/2022
        .build();

FetchRequestRange end = new FetchRequestRange
        .EndTimestampBuilder(2022, 7) // End until the end of 7th month of 2022
        .build();
```
`2. Instantiate AWS Client`
> Refer AWS Configurations sections for creating S3Client 

`3. Instantiate Source Application`
* With Mandatory Fields
```java
SourceApplication app = new SourceApplication.Builder()
        .s3Builder(s3Client, "BUCKET", "CONSUME-TOPIC")
        .kafkaBuilder("BOOTSTRAP-ID", new NewTopic("PRODUCE-TOPIC", <partitions>, (short) 1))
        .range(start, end)
        .build();
```

* With Optional Fields

```java
SourceApplication app = new SourceApplication.Builder()
        .s3Builder(s3Client, "BUCKET", "CONSUME-TOPIC")
        .kafkaBuilder("BOOTSTRAP-ID", new NewTopic("PRODUCE-TOPIC", <partitions>, (short) 1))
        .range(start, end)
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

// Resumes process using any previous cache available
app.resume();

// Replays the rejected cache from any previous runs if present
app.replayRejectedCache();
```

> <b>[Click here](https://github.com/prabh1601/Kafka-Archiver/blob/LocalStorageBatching/src/main/java/com/prabh/CodeExamples/FetcherRun.java) for complete code example</b>
</details>
</details>
