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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class ReplicatorTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicatorTest.class);

    private static final String PEER_NAME = "rocketmq.hbase";

    private final TableName TABLE_NAME = TableName.valueOf(super.TABLE_NAME);
    private final String ROWKEY = "rk-%s";
    private final String COLUMN_FAMILY = "d";
    private final String QUALIFIER = "q";
    private final String VALUE = "v";

    private int batchSize = 100;

    /**
     * This method tests the replicator by writing data from hbase to rocketmq and reading it back.
     */
    @Test
    public void testCustomReplicationEndpoint() throws Exception {
        try {
            createTestTable();

            Map<TableName, List<String>> tableCfs = new HashMap<>();
            List<String> cfs = new ArrayList<>();
            cfs.add(COLUMN_FAMILY);
            tableCfs.put(TABLE_NAME, cfs);
            addPeer(utility.getConfiguration(), PEER_NAME, tableCfs);

            // wait for new peer to be added
            Thread.sleep(500);

            final int numberOfRecords = 10;
            final Transaction inTransaction = insertData(numberOfRecords);

            // wait for data to be replicated
            Thread.sleep(500);

            DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(RocketMQProducer.PRODUCER_GROUP_NAME);
            consumer.setNamesrvAddr(NAMESERVER);
            consumer.setMessageModel(MessageModel.valueOf("BROADCASTING"));
            consumer.registerMessageQueueListener(ROCKETMQ_TOPIC, null);
            consumer.start();

            int receiveNum = 0;
            String receiveMsg = null;
            Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(ROCKETMQ_TOPIC);
            for (MessageQueue queue : queues) {
                long offset = getMessageQueueOffset(consumer, queue);
                PullResult pullResult = consumer.pull(queue, null, offset, batchSize);

                if (pullResult.getPullStatus() == PullStatus.FOUND) {
                    for (MessageExt message : pullResult.getMsgFoundList()) {
                        byte[] body = message.getBody();
                        receiveMsg = new String(body, "UTF-8");
                        // String[] receiveMsgKv = receiveMsg.split(",");
                        // msgs.remove(receiveMsgKv[1]);
                        LOGGER.info("receive message : {}", receiveMsg);
                        receiveNum++;
                    }
                    long nextBeginOffset = pullResult.getNextBeginOffset();
                    consumer.updateConsumeOffset(queue, offset);
                }
            }
            LOGGER.info("receive message num={}", receiveNum);

            // wait for processQueueTable init
            Thread.sleep(1000);

            consumer.shutdown();


            // TODO complete assert equals

        } finally {
            removePeer();
        }

    }

    private long getMessageQueueOffset(DefaultMQPullConsumer consumer, MessageQueue queue) throws MQClientException {
        long offset = consumer.fetchConsumeOffset(queue, false);
        if (offset < 0) {
            offset = 0;
        }
        return offset;
    }

    /**
     * Creates the hbase table with a scope set to Global
     *
     * @throws IOException
     */
    private void createTestTable() throws IOException {
        try (HBaseAdmin hBaseAdmin = utility.getHBaseAdmin()) {
            final HTableDescriptor hTableDescriptor = new HTableDescriptor(TABLE_NAME);
            final HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(COLUMN_FAMILY);
            hColumnDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
            hTableDescriptor.addFamily(hColumnDescriptor);
            hBaseAdmin.createTable(hTableDescriptor);
        }
        utility.waitUntilAllRegionsAssigned(TABLE_NAME);
    }

    /**
     * Adds data to the previously created HBase table
     *
     * @throws IOException
     */
    private Transaction insertData(int numberOfRecords) throws IOException {
        final Transaction transaction = new Transaction(numberOfRecords);
        try(Table hTable = ConnectionFactory.createConnection(utility.getConfiguration()).getTable(TABLE_NAME)) {
            for(int i = 0; i < numberOfRecords; i++) {
                final byte[] rowKey = toBytes(String.format(ROWKEY, i));
                final byte[] family = toBytes(COLUMN_FAMILY);
                final Put put = new Put(rowKey);
                put.addColumn(family, toBytes(QUALIFIER), toBytes(VALUE));
                hTable.put(put);

                List<Cell> cells = put.getFamilyCellMap().get(family);
                transaction.addRow(super.TABLE_NAME, rowKey, cells);
            }
        }
        return transaction;
    }

    /**
     * Removes the peer
     *
     * @throws IOException
     * @throws ReplicationException
     */
    private void removePeer() throws IOException, ReplicationException {
        try(ReplicationAdmin replicationAdmin = new ReplicationAdmin(utility.getConfiguration())) {
            replicationAdmin.removePeer(PEER_NAME);
        }
    }

}
