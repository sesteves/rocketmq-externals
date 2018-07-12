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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MessageProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private RocketMQConsumer consumer;

    private HBaseClient hbaseClient;

    /**
     * @param config
     */
    public MessageProcessor(Config config) {
        consumer = new RocketMQConsumer(config);
        hbaseClient = new HBaseClient();
    }

    public void start() throws MQClientException, IOException {
        consumer.start();
        hbaseClient.start();
        doProcess();
    }

    /**
     *
     */
    private void doProcess() {
        while (true) {

            try {
                final Map<String, List<MessageExt>> messagesPerTopic = consumer.pull();
                for(Map.Entry<String, List<MessageExt>> entry : messagesPerTopic.entrySet()) {
                    final String topic = entry.getKey();
                    final List<MessageExt> messages = entry.getValue();
                    hbaseClient.put(topic, messages);
                }


            } catch (Exception e) {
                logger.error("Error while processing messages.", e);
            }
        }
    }

}
