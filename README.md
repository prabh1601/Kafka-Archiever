![](https://img.shields.io/badge/Made%20With-%20java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)
# Kafka-Archiver
## Problem Statement
Kafka stores data in partitions with a specific retention period, it might happen that at some point, 
replaying data between a specific timeperiod is required. This application incorporates this tasks in
two independant components.

The goals of this application are two fold
1. Archive data from Kafka clusters to S3 buckets
2. Retrieve data from S3 that belongs in between the input timeperiod and push it to Kafka cluster



<b>[Temporary Usecase Examples](https://github.com/prabh1601/Kafka-Archiver/tree/LocalStorageBatching/src/main/java/com/prabh/CodeExamples)</b>
<details>
<summary> Timeline of the Project </summarY>

Although this might not be accurate to point, but still gives a rough image
##### Week 1
* Induction 
* Learning Java (Basics)
* Getting to know about Kafka

##### Week 2 
* Learning about Kafka Internals
* Learning Threads/Concurrency
* Writing a decoupled Kafka Streamer and separating them into tasks

##### Week 3 
* Try different models to push data to s3
* Implement individual services/client structures for more streamline workflow of the application

##### Week 4
* Handle upload process better and put constraints to potential memory overflow
* Fetching data between a given time period from s3
* Implement producer service for new kafka topic

##### Week 5
* Redo download service as list instead of downloading whole package at once 
* Redo Partition Batching service

##### Week 6
* Incorporate "in memory" options for both part of the project
* Incorporate dead letter queue for producer service
* Write First Draft for the blog

##### Week 7 (Ongoing)
* Add Last finish stuff
* Stresstesting and writing Unit tests

##### Future
* (week 8) Presentation 
</details>
