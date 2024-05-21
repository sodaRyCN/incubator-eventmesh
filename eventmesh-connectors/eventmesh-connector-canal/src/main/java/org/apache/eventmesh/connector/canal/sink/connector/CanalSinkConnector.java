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

package org.apache.eventmesh.connector.canal.sink.connector;

import javafx.fxml.LoadException;

import org.apache.eventmesh.common.config.connector.Config;

import org.apache.eventmesh.common.config.connector.rdb.canal.CanalSinkConfig;
import org.apache.eventmesh.connector.canal.CanalConnectRecord;
import org.apache.eventmesh.connector.canal.DatabaseConnection;
import org.apache.eventmesh.openconnect.api.ConnectorCreateService;
import org.apache.eventmesh.openconnect.api.connector.ConnectorContext;
import org.apache.eventmesh.openconnect.api.connector.SinkConnectorContext;
import org.apache.eventmesh.openconnect.api.sink.Sink;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.ConnectRecord;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CanalSinkConnector implements Sink, ConnectorCreateService<Sink> {

    private CanalSinkConfig sinkConfig;

    private JdbcTemplate jdbcTemplate;

    @Override
    public Class<? extends Config> configClass() {
        return CanalSinkConfig.class;
    }

    @Override
    public void init(Config config) throws Exception {
        // init config for canal source connector
        this.sinkConfig = (CanalSinkConfig) config;
    }

    @Override
    public void init(ConnectorContext connectorContext) throws Exception {
        // init config for canal source connector
        SinkConnectorContext sinkConnectorContext = (SinkConnectorContext) connectorContext;
        this.sinkConfig = (CanalSinkConfig) sinkConnectorContext.getSinkConfig();
        DatabaseConnection.sinkConfig = this.sinkConfig;
        DatabaseConnection.initSinkConnection();
        jdbcTemplate = new JdbcTemplate(DatabaseConnection.sinkDataSource);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void commit(ConnectRecord record) {

    }

    @Override
    public String name() {
        return this.sinkConfig.getSinkConnectorConfig().getConnectorName();
    }

    @Override
    public void stop() {

    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) {
        for (ConnectRecord connectRecord : sinkRecords) {
            List<CanalConnectRecord> canalConnectRecordList = (List<CanalConnectRecord>) connectRecord.getData();
            if (isDdlDatas(canalConnectRecordList)) {
                doDdl(canalConnectRecordList);
            } else {
                for (CanalConnectRecord canalConnectRecord : canalConnectRecordList) {
                    if (sinkConfig.getSinkConnectorConfig().getSchemaName().equals(canalConnectRecord.getSchemaName()) &&
                        sinkConfig.getSinkConnectorConfig().getTableName().equals(canalConnectRecord.getTableName())) {


                    }
                }
            }

        }
    }

    @Override
    public Sink create() {
        return new CanalSinkConnector();
    }

    /**
     * 分析整个数据，将datas划分为多个批次. ddl sql前的DML并发执行，然后串行执行ddl后，再并发执行DML
     *
     * @return
     */
    private boolean isDdlDatas(List<CanalConnectRecord> canalConnectRecordList) {
        boolean result = false;
        for (CanalConnectRecord canalConnectRecord : canalConnectRecordList) {
            result |= canalConnectRecord.getEventType().isDdl();
            if (result && !canalConnectRecord.getEventType().isDdl()) {
                throw new RuntimeException("ddl/dml can't be in one batch, it's may be a bug , pls submit issues.");
            }
        }
        return result;
    }

    /**
     * 执行ddl的调用，处理逻辑比较简单: 串行调用
     *
     * @param canalConnectRecordList
     */
    private void doDdl(List<CanalConnectRecord> canalConnectRecordList) {
        for (final CanalConnectRecord record : canalConnectRecordList) {
            if (!sinkConfig.getSinkConnectorConfig().getSchemaName().equals(record.getSchemaName()) ||
                !sinkConfig.getSinkConnectorConfig().getTableName().equals(record.getTableName())) {
                continue;
            }
            try {
                Boolean result = jdbcTemplate.execute(new StatementCallback<Boolean>() {

                    public Boolean doInStatement(Statement stmt) throws SQLException, DataAccessException {
                        boolean result = true;
                        if (StringUtils.isNotEmpty(record.getDdlSchemaName())) {
                            // 如果mysql，执行ddl时，切换到在源库执行的schema上
                            // result &= stmt.execute("use " +
                            // data.getDdlSchemaName());

                            // 解决当数据库名称为关键字如"Order"的时候,会报错,无法同步
                            result &= stmt.execute("use `" + record.getDdlSchemaName() + "`");
                        }
                        result &= stmt.execute(record.getSql());
                        return result;
                    }
                });

            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

        }
    }
}
