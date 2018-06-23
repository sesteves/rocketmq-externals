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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class ReplicatorTest extends BaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicatorTest.class);

    private static final String PEER_NAME = "rocketmq.hbase";

    private final TableName TABLE_NAME = TableName.valueOf("testings");
    private final String ROWKEY = "rk-%s";
    private final String COLUMN_FAMILY = "d";
    private final String QUALIFIER = "q";
    private final String VALUE = "v";

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

            final int numberOfRecords = 10;
            insertData(numberOfRecords);





        } finally {
            removePeer();
        }

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
    private void insertData(int numberOfRecords) throws IOException {
        try(Table hTable = ConnectionFactory.createConnection(utility.getConfiguration()).getTable(TABLE_NAME)) {
            for(int i = 0; i < numberOfRecords; i++) {
                Put put = new Put(toBytes(String.format(ROWKEY, i)));
                put.addColumn(toBytes(COLUMN_FAMILY), toBytes(QUALIFIER), toBytes(VALUE));
                hTable.put(put);
            }
        }
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
