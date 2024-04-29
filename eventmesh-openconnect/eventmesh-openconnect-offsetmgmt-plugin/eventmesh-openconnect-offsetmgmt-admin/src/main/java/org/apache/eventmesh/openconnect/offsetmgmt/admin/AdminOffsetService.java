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

import org.apache.eventmesh.common.config.connector.offset.OffsetStorageConfig;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc.AdminServiceBlockingStub;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc.AdminServiceStub;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.Position;
import org.apache.eventmesh.common.remote.request.ReportPositionRequest;
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
import java.util.Objects;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.Any;
import com.google.protobuf.UnsafeByteOperations;

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

    private String jobId;

    private JobState jobState;


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
        Map<ConnectorRecordPartition, RecordOffset> recordMap = positionStore.getKVMap();

        List<Map<String, Object>> recordToSyncList = new ArrayList<>();
        for (Map.Entry<ConnectorRecordPartition, RecordOffset> entry : recordMap.entrySet()) {
            Map<String, Object> synchronizeMap = new HashMap<>();
            synchronizeMap.put("connectorRecordPartition", entry.getKey());
            synchronizeMap.put("recordOffset", entry.getValue());
            recordToSyncList.add(synchronizeMap);
        }

        ReportPositionRequest reportPositionRequest = new ReportPositionRequest();
        reportPositionRequest.setJobID(jobId);
        reportPositionRequest.setState(jobState);
        Position<Map<String, Object>> position = new Position<>();
        position.setRecordPositionList(recordToSyncList);
        reportPositionRequest.setPosition(position);

        Metadata metadata = Metadata.newBuilder()
            .setType(ReportPositionRequest.class.getSimpleName())
            .build();
        Payload payload = Payload.newBuilder()
            .setMetadata(metadata)
            .setBody(Any.newBuilder().setValue(UnsafeByteOperations.
                unsafeWrap(Objects.requireNonNull(JsonUtils.toJSONBytes(reportPositionRequest)))).build())
            .build();
        requestObserver.onNext(payload);
    }

    @Override
    public Map<ConnectorRecordPartition, RecordOffset> getPositionMap() {
        // get from memory storage first
        if (positionStore.getKVMap() == null || positionStore.getKVMap().isEmpty()) {
            //TODO: get position from adminService
        }
        log.info("memory position map {}", positionStore.getKVMap());
        return positionStore.getKVMap();
    }

    @Override
    public RecordOffset getPosition(ConnectorRecordPartition partition) {
        // get from memory storage first
        if (positionStore.get(partition) == null) {
            //TODO: get position from adminService
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
        this.jobState = JobState.INIT;
        this.jobId = offsetStorageConfig.getExtensions().get("jobId");
    }
}
