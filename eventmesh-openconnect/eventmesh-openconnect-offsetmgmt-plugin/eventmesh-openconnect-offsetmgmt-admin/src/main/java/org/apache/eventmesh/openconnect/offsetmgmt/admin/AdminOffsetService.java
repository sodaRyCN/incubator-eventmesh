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

package org.apache.eventmesh.openconnect.offsetmgmt.admin;

import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.common.config.offset.OffsetStorageConfig;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc.AdminServiceBlockingStub;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc.AdminServiceStub;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.RecordOffset;
import org.apache.eventmesh.openconnect.offsetmgmt.api.storage.ConnectorRecordPartition;
import org.apache.eventmesh.openconnect.offsetmgmt.api.storage.KeyValueStore;
import org.apache.eventmesh.openconnect.offsetmgmt.api.storage.MemoryBasedKeyValueStore;
import org.apache.eventmesh.openconnect.offsetmgmt.api.storage.OffsetManagementService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdminOffsetService implements OffsetManagementService {

    private String adminServerAddr;

    private ManagedChannel channel;

    private AdminServiceStub adminServiceStub;

    private AdminServiceBlockingStub adminServiceBlockingStub;

    StreamObserver<Payload> responseObserver;

    StreamObserver<Payload> requestObserver;

    public KeyValueStore<ConnectorRecordPartition, RecordOffset> positionStore;


    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void configure(OffsetStorageConfig config) {
        OffsetManagementService.super.configure(config);
    }

    @Override
    public void persist() {

    }

    @Override
    public void load() {

    }

    @Override
    public void synchronize() {
        Metadata metadata = Metadata.newBuilder()
            .setType()
            .build();
        Payload payload = Payload.newBuilder()
            .setMetadata()
            .setBody()
            .build();
    }

    @Override
    public Map<ConnectorRecordPartition, RecordOffset> getPositionMap() {
        // get from memory storage first
        if (positionStore.getKVMap() == null || positionStore.getKVMap().isEmpty()) {
            // get position from adminService
            Metadata metadata = Metadata.newBuilder()
                .setType()
                .build();
            Payload payload = Payload.newBuilder()
                .setMetadata()
                .setBody()
                .build();

            adminServiceBlockingStub.invoke()
            try {
                Map<ConnectorRecordPartition, RecordOffset> configMap = JsonUtils.parseTypeReferenceObject(configService.getConfig(dataId, group, 5000L),
                    new TypeReference<Map<ConnectorRecordPartition, RecordOffset>>() {
                    });
                log.info("position map {}", configMap);
                return configMap;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        log.info("memory position map {}", positionStore.getKVMap());
        return positionStore.getKVMap();
    }

    @Override
    public RecordOffset getPosition(ConnectorRecordPartition partition) {
        // get from memory storage first
        if (positionStore.get(partition) == null) {
            // get position from adminService
            try {
                Map<ConnectorRecordPartition, RecordOffset> recordMap = JacksonUtils.toObj(configService.getConfig(dataId, group, 5000L),
                    new TypeReference<Map<ConnectorRecordPartition, RecordOffset>>() {
                    });
                log.info("nacos record position {}", recordMap.get(partition));
                return recordMap.get(partition);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        log.info("memory record position {}", positionStore.get(partition));
        return positionStore.get(partition);
    }

    @Override
    public void putPosition(Map<ConnectorRecordPartition, RecordOffset> positions) {
        positionStore.putAll(positions);
    }

    @Override
    public void putPosition(ConnectorRecordPartition partition, RecordOffset position) {
        positionStore.put(partition, position);
    }

    @Override
    public void removePosition(List<ConnectorRecordPartition> partitions) {
        if (partitions == null) {
            return;
        }
        for (ConnectorRecordPartition partition : partitions) {
            positionStore.remove(partition);
        }
    }

    @Override
    public void initialize(OffsetStorageConfig offsetStorageConfig) {
        this.adminServerAddr = offsetStorageConfig.getOffsetStorageAddr();
        this.channel = ManagedChannelBuilder.forTarget(adminServerAddr)
            .usePlaintext()
            .build();
        this.adminServiceStub = AdminServiceGrpc.newStub(channel).withWaitForReady();
        this.adminServiceBlockingStub = AdminServiceGrpc.newBlockingStub(channel).withWaitForReady();

        responseObserver = new StreamObserver<Payload>() {
            @Override
            public void onNext(Payload response) {
                log.info("receive message: {} ", response);
            }

            @Override
            public void onError(Throwable t) {
                log.error("receive error message: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("finished receive message and completed");
            }
        };

        requestObserver = adminServiceStub.invokeBiStream(responseObserver);

        this.positionStore = new MemoryBasedKeyValueStore<>();
        Map<ConnectorRecordPartition, RecordOffset> initialRecordOffsetMap = JsonUtils.parseTypeReferenceObject(offsetStorageConfig.getExtensions().get("offset"),
            new TypeReference<Map<ConnectorRecordPartition, RecordOffset>>(){
            });
        log.info("init record offset {}", initialRecordOffsetMap);
        positionStore.putAll(initialRecordOffsetMap);
    }
}
