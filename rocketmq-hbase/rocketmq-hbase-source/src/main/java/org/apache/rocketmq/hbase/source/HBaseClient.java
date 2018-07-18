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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

/**
 *
 */
public class HBaseClient {

    private static final byte[] COLUMN_FAMILY = toBytes("MESSAGE");

    private static final Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private String zookeeperAddress;

    private Connection connection;

    public HBaseClient(Config config) {
        this.zookeeperAddress = config.getZookeeperAddress();
    }

    public void start() throws IOException {
        final Configuration hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", zookeeperAddress);
        connection = ConnectionFactory.createConnection(hbaseConfig);
    }

    /**
     *
     * @param tableName
     * @param messages
     * @throws IOException
     */
    public void put(String tableName, List<MessageExt> messages) throws IOException {

        try(Table table = connection.getTable(TableName.valueOf(tableName))) {
            final List<Put> puts = new ArrayList<>();

            for(MessageExt msg : messages) {
                final Put put = new Put(toBytes(msg.getMsgId()));
                put.addColumn(COLUMN_FAMILY, null, msg.getBody());
                puts.add(put);
            }

            table.put(puts);
        }
    }

    /**
     *
     * @throws IOException
     */
    public void stop() throws IOException {
        connection.close();
    }
}
