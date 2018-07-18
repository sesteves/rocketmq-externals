# RocketMQ-HBase Source

## Overview

This project replicates RocketMQ topics to HBase tables.

## Pre-requisites
- HBase 1.2+
- JDK 1.8+
- RocketMQ 4.0.0+ 

## Assumptions

- Each specified RocketMQ topic is mapped to a HBase table with the same name
- The HBase tables already exist

## Properties

Have the below properties set in `rocketmq_hbase.conf`

|key               |nullable|default    |description|
|------------------|--------|-----------|-----------|
| nameserver     |false   |           |RocketMQ name server address (e.g.,127.0.0.1:9876)|
| consumerGroup | true     |"HBASE_CONSUMER_GROUP"| The consumer group name|
| messageModel  | true     | "BROADCASTING"       |RocketMQ message model, 'BROADCASTING' or 'CLUSTERING'|
| topics    |  false |  | A comma separated list of RocketMQ topics to replicate to HBase (e.g., topic1,topic2,topic3) |
| zookeeperAddress | true | localhost | A comma separated list of the IP addresses of all ZooKeeper servers in the cluster | 
| batchSize     | true     | 32                   | The maximum number of messages to be consumed in batch from RocketMQ|
| pullInterval | true | 1000 | Time in milliseconds to wait between consecutive pulls |