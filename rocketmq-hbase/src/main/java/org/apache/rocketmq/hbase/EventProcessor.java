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

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventProcessor.class);

    private Replicator replicator;

    private Config config;

    private BlockingQueue<Event> queue = new LinkedBlockingQueue<>(100);

    private EventListener eventListener;

    private Transaction transaction;

    public EventProcessor(Replicator replicator) {

        this.replicator = replicator;
        this.config = replicator.getConfig();
    }


    public void start() throws Exception {
        initDataSource();

        eventListener = new EventListener(queue);


        LOGGER.info("Started.");
    }

    private void doProcess() {
        while (true) {
            try {
                Event event = queue.poll(1000, TimeUnit.MILLISECONDS);
                // TODO check if event is null

                switch(event.getType()) {
                    case PUT:
                        addRow("WRITE", null);
                        break;
                    case DELETE:
                        addRow("DELETE", null);
                        break;
                }

            } catch(Exception e) {
                LOGGER.error("Error processing log.", e);
            }
        }
    }


    private void addRow(String type, Serializable[] row) {
        if (transaction == null) {
            transaction = new Transaction(config);
        }

        transaction.addRow(rowKey, columns);
        replicator.commit(transaction, false);

        transaction = new Transaction(config);
    }

    private void initDataSource() throws Exception {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);

        try {
            Table table = connection.getTable(TableName.valueOf(""));
        } finally {
            connection.close();
        }
    }
}
