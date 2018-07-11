/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.hbase.source;

import java.util.List;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RocketMQConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RocketMQConsumer.class);

    private DefaultMQPullConsumer consumer;

    private String namesrvAddr;

    private String topic;

    private MessageModel messageModel;

    private Set<String> topics;

    private int batchSize;

    public RocketMQConsumer(Config config) {
        this.namesrvAddr = config.getNameserver();
        this.messageModel = MessageModel.valueOf(config.getMessageModel());
        this.topics = config.getTopics();
        this.batchSize = config.getBatchSize();
    }

    public void start() throws MQClientException {
        consumer = new DefaultMQPullConsumer();
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setMessageModel(messageModel);
        consumer.setRegisterTopics(topics);
        consumer.start();
    }

    public void pull() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {

        Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
        for (MessageQueue queue : queues) {
            long offset = getMessageQueueOffset(queue);
            PullResult pullResult = consumer.pull(queue, null, offset, batchSize);
        }

    }

    private long getMessageQueueOffset(MessageQueue queue) throws MQClientException {
        long offset = consumer.fetchConsumeOffset(queue, false);
        if (offset < 0) {
            offset = 0;
        }

        return offset;
    }

    public void stop() {
        consumer.shutdown();
    }
}
