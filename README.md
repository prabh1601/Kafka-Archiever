![](https://img.shields.io/badge/Made%20With-%20java-%23ED8B00.svg?style=for-the-badge&logo=java&logoColor=white)

# Kafka-Archiver
The goals of this application are two fold
1. Archive data from Kafka clusters to S3 buckets
2. Retrieve data from S3 that belongs in between the input timeperiod and push it to Kafka cluster


## Timeline
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

##### Ongoing (Week 4)
* Handle upload process better and put constraints to potential memory overflow
* Incoporate various compression and upload options ?
* Complete Sink Connector (part 1 of project)

##### Future
* Implement Fetching data between a given time period from s3
* Pushing to Another Kafka Cluster : Producer 
* (week 7) Write Blog
* (week 8) Presentation 
