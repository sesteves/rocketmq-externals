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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseTest {

    private static NamesrvController namesrvController;

    private static BrokerController brokerController;

    private static String nameServer = "localhost:9876";

    protected HBaseTestingUtility utility;

    protected int numRegionServers;

    @Before
    public void setUp() throws Exception {
        final Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("replication.stats.thread.period.seconds", 5);
        hbaseConf.setLong("replication.sleep.before.failover", 2000);
        hbaseConf.setInt("replication.source.maxretriesmultiplier", 10);
        hbaseConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);

        // Add RocketMQ properties - we prefix each property with 'rocketmq'
        addRocketMQProperties(hbaseConf);

        utility = new HBaseTestingUtility(hbaseConf);
        utility.startMiniCluster();
        numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();

        utility = new HBaseTestingUtility(hbaseConf);
        utility.startMiniCluster();
        numRegionServers = utility.getHBaseCluster().getRegionServerThreads().size();

        // setup RocketMQ



    }

    /**
     * Add RocketMQ properties to {@link Configuration}
     *
     * @param hbaseConf
     */
    private void addRocketMQProperties(Configuration hbaseConf) {
        hbaseConf.set("rocketmq.namesrv.addr", "localhost:9876");
        hbaseConf.set("rocketmq.topic", "hbase-rocketmq-topic-test");
        hbaseConf.set("rocketmq.hbase.tables", "hbase-rocketmq-test");
    }

    /**
     *
     * @param configuration
     * @param peerName
     * @param tableCFs
     * @throws ReplicationException
     * @throws IOException
     */
    protected void addPeer(final Configuration configuration,String peerName, Map<TableName, List<String>> tableCFs)
        throws ReplicationException, IOException {
        try (ReplicationAdmin replicationAdmin = new ReplicationAdmin(configuration)) {
            ReplicationPeerConfig peerConfig = new ReplicationPeerConfig()
                .setClusterKey(ZKConfig.getZooKeeperClusterKey(configuration))
                .setReplicationEndpointImpl(Replicator.class.getName());

            replicationAdmin.addPeer(peerName, peerConfig, tableCFs);
        }
    }


    @BeforeClass
    public static void startMQ() throws Exception {
        startNamesrv();
        startBroker();

        Thread.sleep(2000);
    }

    private static void startNamesrv() throws Exception {

        NamesrvConfig namesrvConfig = new NamesrvConfig();
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(9876);

        namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        boolean initResult = namesrvController.initialize();
        if (!initResult) {
            namesrvController.shutdown();
            throw new Exception();
        }
        namesrvController.start();
    }

    private static void startBroker() throws Exception {

        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        BrokerConfig brokerConfig = new BrokerConfig();
        brokerConfig.setNamesrvAddr(nameServer);
        brokerConfig.setBrokerId(MixAll.MASTER_ID);
        NettyServerConfig nettyServerConfig = new NettyServerConfig();
        nettyServerConfig.setListenPort(10911);
        NettyClientConfig nettyClientConfig = new NettyClientConfig();
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();

        brokerController = new BrokerController(brokerConfig, nettyServerConfig, nettyClientConfig, messageStoreConfig);
        boolean initResult = brokerController.initialize();
        if (!initResult) {
            brokerController.shutdown();
            throw new Exception();
        }
        brokerController.start();
    }



    @After
    public void tearDown() throws Exception {
        if(kafkaServer != null) {
            kafkaServer.shutdown();
        }
        if(utility != null) {
            utility.shutdownMiniCluster();
        }
    }
}
