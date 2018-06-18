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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.groupingBy;

/**
 *
 */
public class HBaseEndpoint extends BaseReplicationEndpoint {

    private static final String ROCKETMQ_TOPICS_PARAM = "rocketmq.hbase.tables.topics";

    private static final String ROCKETMQ_NAMESRV_ADDR_PARAM = "rocketmq.namesrv.addr";


    private static final String PRODUCER_GROUP_NAME = "HBASE_PRODUCER_GROUP";

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseEndpoint.class);

    private DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP_NAME);

    private Set<String> topics = Sets.newHashSet();

    /**
     *
     */
    public HBaseEndpoint() {
        super();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void init(Context context) throws IOException {
        super.init(context);
        LOGGER.info("HBaseEndpoint initialized");
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doStart() {
        LOGGER.info("HBase replication to RocketMQ started");

        final Configuration config = ctx.getConfiguration();
        final String topicsParam = config.get(ROCKETMQ_TOPICS_PARAM);
        if (topicsParam == null) {
            // TODO throw exception ?
        }
        topics = new HashSet<>(Arrays.asList(topicsParam.split(",")));


        final String namesrvAddr = config.get(ROCKETMQ_NAMESRV_ADDR_PARAM);
        if(namesrvAddr == null) {
            // TODO throw exception ?
        }

        try {
            producer.setNamesrvAddr(namesrvAddr);
            producer.start();
        } catch (MQClientException e) {
            // TODO
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doStop() {
        LOGGER.info("HBase replication to RocketMQ stopped.");
        producer.shutdown();
        notifyStopped();
    }

    /**
     * {@inheritDoc}
     */
    @Override public UUID getPeerUUID() {
        return UUID.randomUUID();
    }

    /**
     * {@inheritDoc}
     */
//    @Override public boolean replicate(ReplicateContext context) {
//        final List<WAL.Entry> entries = context.getEntries();
//
//        final Map<String, List<WAL.Entry>> entriesByTable = entries.stream()
//                .filter(entry ->  topics.contains(entry.getKey().getTablename().getNameAsString()))
//                .collect(groupingBy(entry -> entry.getKey().getTablename().getNameAsString()));
//
//        Transaction transaction = new Transaction(ctx.getConfiguration());
//
//        // replicate data to rocketmq in parallel
//        entriesByTable.entrySet().stream().forEach(entry -> {
//            final String tableName = entry.getKey();
//            final List<WAL.Entry> tableEntries = entry.getValue();
//
//            tableEntries.forEach(tblEntry -> {
//                List<Cell> cells = tblEntry.getEdit().getCells();
//
//                // group entries by the row key
//                Map<byte[], List<Cell>> columnsByRow = cells.stream().collect(groupingBy(CellUtil::cloneRow));
//
//                columnsByRow.entrySet().stream().forEach(rowCols -> {
//                    final byte[] rowKey = rowCols.getKey();
//                    final List<Cell> columns = rowCols.getValue();
//
//                    // final HRow row = TO_HROW.apply(rowKey, columns);
//
//                    if (!transaction.addRow(rowKey, columns)) {
//                        //                    Message message = new Message(tableName, json.getBytes("UTF-8"));
//                        try {
//                            producer.send(null);
//                        } catch (Exception e) {
//                            // TODO
//                            e.printStackTrace();
//                        }
//                        transaction = new Transaction(ctx.getConfiguration());
//                    }
//
//                });
//            });
//
//        });
//
//        return true;
//    }

    /**
     * {@inheritDoc}
     */
    @Override public boolean replicate(ReplicateContext context) {
        final List<WAL.Entry> entries = context.getEntries();



        return false;
    }


}
