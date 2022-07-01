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
