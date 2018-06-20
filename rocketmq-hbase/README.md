# RocketMQ-HBase

## Overview

This project replicates HBase tables to RocketMQ topics.

## Pre-requisites
- HBase 1.4.4
- JDK 1.8+
- RocketMQ 4.0.0+ 

## Properties
|key               |nullable|default    |description|
|------------------|--------|-----------|-----------|
|mqNamesrvAddr     |false   |           |RocketMQ name server address (e.g.,127.0.0.1:9876)|
|mqTopics          |false   |           |RocketMQ topic names separated by comma. (e.g., topic1,topic2,topic3)|


## Deployment
1. Add rocketmq-hbase-X.Y-SNAPSHOT.jar and hbase-site.xml with the required properties to all the HBase Region servers classpath and restart them.

2. At HBase shell, run the following commands.

```bash
hbase> create 'test', {NAME => 'd', REPLICATION_SCOPE => '1'}
hbase> add_peer 'rocketmq-repl', ENDPOINT_CLASSNAME => 'org.apache.rocketmq.hbase.Replicator'
hbase> put 'test', 'r1', 'd', 'value'
```
