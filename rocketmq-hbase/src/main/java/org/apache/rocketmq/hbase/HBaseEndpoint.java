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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class HBaseEndpoint extends BaseReplicationEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseEndpoint.class);

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

        final Configuration hdfsConfig = ctx.getConfiguration();


    }

    /**
     * {@inheritDoc}
     */
    @Override protected void doStop() {

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
    @Override public boolean replicate(ReplicateContext context) {
        final List<WAL.Entry> entries = context.getEntries();

//        final Map<String, List<WAL.Entry>> entriesByTable = entries.stream()
//            .filter(entry -> topicNameFilter.test(entry.getKey().getTablename().getNameAsString()))
//            .collect(groupingBy(entry -> entry.getKey().getTablename().getNameAsString()));

        return false;
    }
}
