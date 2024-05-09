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
import org.apache.eventmesh.openconnect.api.ConnectorCreateService;
import org.apache.eventmesh.openconnect.api.connector.ConnectorContext;
import org.apache.eventmesh.openconnect.api.connector.SourceConnectorContext;
import org.apache.eventmesh.openconnect.api.source.Source;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.ConnectRecord;

import java.util.List;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CanalSourceConnector implements Source, ConnectorCreateService<Source> {

    private CanalSourceConfig sourceConfig;

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
    }


    @Override
    public void start() throws Exception {
    }


    @Override
    public void commit(ConnectRecord record) {

    }

    @Override
    public String name() {
        return this.sourceConfig.getSourceConnectorConfig().getName();
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
