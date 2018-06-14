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
package org.apache.rocketmq.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(Replicator.class);

    private Config config;

    private EventProcessor eventProcessor;

    private RocketMQProducer rocketMQProducer;

    public static void main(String[] args) {

        Replicator replicator = new Replicator();
        replicator.start();
    }

    public void start() {
        try {
            config = new Config();

            rocketMQProducer = new RocketMQProducer(config);
            rocketMQProducer.start();

            eventProcessor = new EventProcessor(this);
            eventProcessor.start();
        }
        catch (Exception e) {
            LOGGER.error("Start error.", e);
            System.exit(1);
        }
    }

    public void commit(Transaction transaction, boolean isComplete) {
        // TODO implement
    }

    public Config getConfig() {
        return config;
    }
}
