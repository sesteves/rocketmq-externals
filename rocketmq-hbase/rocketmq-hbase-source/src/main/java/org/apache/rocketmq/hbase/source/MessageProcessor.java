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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MessageProcessor {

    private Logger logger = LoggerFactory.getLogger(MessageProcessor.class);

    private BlockingQueue<Message> queue;

    private MessageConsumer consumer;

    /**
     *
     * @param config
     */
    public MessageProcessor(Config config) {
        // TODO consider adding queue capacity for performance reasons
        queue = new LinkedBlockingQueue<>();
        consumer = new MessageConsumer(config, queue);
    }

    /**
     *
     */
    private void doProcess() {
        while(true) {

            try {
                Message message = queue.poll(1000, TimeUnit.MILLISECONDS);
                if (message == null) {
                    // checkConnection();
                    continue;
                }




            } catch(Exception e) {
                logger.error("Error while processing message.", e);
            }
        }
    }

}
