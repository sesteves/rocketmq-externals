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
|mqNamesrvAddr     |false   |           |RocketMQ name server address (e.g.,127.0.0.1:9876)|
|mqTopics    |  false |  | List of RocketMQ topics, separated by comma, to replicate to HBase (e.g., topic1,topic2,topic3) | 
