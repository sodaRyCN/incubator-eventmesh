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

package org.apache.eventmesh.connector.canal.source.connector;

import org.apache.eventmesh.common.config.connector.Config;
import org.apache.eventmesh.common.config.connector.rdb.canal.CanalSourceConfig;
import org.apache.eventmesh.connector.canal.DatabaseConnection;
import org.apache.eventmesh.openconnect.api.ConnectorCreateService;
import org.apache.eventmesh.openconnect.api.connector.ConnectorContext;
import org.apache.eventmesh.openconnect.api.connector.SourceConnectorContext;
import org.apache.eventmesh.openconnect.api.source.Source;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.ConnectRecord;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.CanalInstanceWithManager;
import com.alibaba.otter.canal.instance.manager.model.Canal;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.ClusterMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.HAMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.IndexMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.MetaMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.RunMode;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.SourcingType;
import com.alibaba.otter.canal.instance.manager.model.CanalParameter.StorageMode;
import com.alibaba.otter.canal.parse.CanalEventParser;
import com.alibaba.otter.canal.parse.ha.CanalHAController;
import com.alibaba.otter.canal.parse.inbound.mysql.MysqlEventParser;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.sink.AbstractCanalEventSink;
import com.alibaba.otter.canal.sink.CanalEventSink;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CanalSourceConnector implements Source, ConnectorCreateService<Source> {

    private CanalSourceConfig sourceConfig;

    private CanalServerWithEmbedded canalServer;

    private ClientIdentity clientIdentity;

    private String filter;

    private volatile boolean running = false;

    @Override
    public Class<? extends Config> configClass() {
        return CanalSourceConfig.class;
    }

    @Override
    public void init(Config config) throws Exception {
        // init config for canal source connector
        this.sourceConfig = (CanalSourceConfig) config;
    }

    @Override
    public void init(ConnectorContext connectorContext) throws Exception {
        SourceConnectorContext sourceConnectorContext = (SourceConnectorContext) connectorContext;
        this.sourceConfig = (CanalSourceConfig) sourceConnectorContext.getSourceConfig();
        // init source database connection
        DatabaseConnection.sourceConfig = sourceConfig;
        DatabaseConnection.initSourceConnection();

        canalServer = CanalServerWithEmbedded.instance();
        canalServer.setCanalInstanceGenerator(new CanalInstanceGenerator() {
            @Override
            public CanalInstance generate(String destination) {
                Canal canal = buildCanal(sourceConfig);

                CanalInstanceWithManager instance = new CanalInstanceWithManager(canal, filter) {

                    protected CanalHAController initHaController() {
                        return super.initHaController();
                    }

                    protected void startEventParserInternal(CanalEventParser parser, boolean isGroup) {
                        super.startEventParserInternal(parser, isGroup);

                        if (eventParser instanceof MysqlEventParser) {
                            // set eventParser support type
                            ((MysqlEventParser) eventParser).setSupportBinlogFormats("ROW");
                            ((MysqlEventParser) eventParser).setSupportBinlogImages("FULL");
                            MysqlEventParser mysqlEventParser = (MysqlEventParser) eventParser;
                            mysqlEventParser.setParallel(false);

                            CanalHAController haController = mysqlEventParser.getHaController();
                            if (!haController.isStart()) {
                                haController.start();
                            }
                        }
                    }
                };
                return instance;
            }
        });
    }

    private Canal buildCanal(CanalSourceConfig sourceConfig) {
        // 设置下slaveId，保证多个piplineId下重复引用时不重复
        long slaveId = 10000;// 默认基数
        if (sourceConfig.getSlaveId() != null) {
            slaveId = sourceConfig.getSlaveId();
        }

        Canal canal = new Canal();
        canal.setId(sourceConfig.getCanalInstanceId());
        canal.setName(sourceConfig.getDestination());
        canal.setDesc(sourceConfig.getDesc());

        CanalParameter parameter = new CanalParameter();

        parameter.setRunMode(RunMode.EMBEDDED);
        parameter.setClusterMode(ClusterMode.STANDALONE);
        parameter.setMetaMode(MetaMode.MEMORY);
        parameter.setHaMode(HAMode.HEARTBEAT);
        parameter.setIndexMode(IndexMode.MEMORY);
        parameter.setStorageMode(StorageMode.MEMORY);
        parameter.setMemoryStorageBufferSize(32 * 1024);

        parameter.setSourcingType(SourcingType.MYSQL);
        parameter.setDbAddresses(Collections.singletonList(new InetSocketAddress(sourceConfig.getSourceConnectorConfig().getDbAddress(),
            sourceConfig.getSourceConnectorConfig().getDbPort())));
        parameter.setDbUsername(sourceConfig.getSourceConnectorConfig().getUserName());
        parameter.setDbPassword(sourceConfig.getSourceConnectorConfig().getPassWord());
//        parameter.setPositions();
//        Arrays.asList("{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}",
//            "{\"journalName\":\"mysql-bin.000001\",\"position\":6163L,\"timestamp\":1322803601000L}")

        parameter.setSlaveId(slaveId);

        parameter.setDefaultConnectionTimeoutInSeconds(30);
        parameter.setConnectionCharset("UTF-8");
        parameter.setConnectionCharsetNumber((byte) 33);
        parameter.setReceiveBufferSize(8 * 1024);
        parameter.setSendBufferSize(8 * 1024);

        // heartbeat detect
        parameter.setDetectingEnable(false);

        parameter.setDdlIsolation(sourceConfig.isDdlSync());
        parameter.setFilterTableError(sourceConfig.isFilterTableError());
        parameter.setMemoryStorageRawEntry(false);

        canal.setCanalParameter(parameter);
        return canal;
    }


    @Override
    public void start() throws Exception {
        if (running) {
            return;
        }
        canalServer.start();

        canalServer.start(sourceConfig.getDestination());
        this.clientIdentity = new ClientIdentity(sourceConfig.getDestination(), sourceConfig.getClientId(), filter);
        canalServer.subscribe(clientIdentity);

        running = true;
    }


    @Override
    public void commit(ConnectRecord record) {

    }

    @Override
    public String name() {
        return this.sourceConfig.getSourceConnectorConfig().getConnectorName();
    }

    @Override
    public void stop() {

    }

    @Override
    public List<ConnectRecord> poll() {
        return null;
    }


    @Override
    public Source create() {
        return new CanalSourceConnector();
    }
}
