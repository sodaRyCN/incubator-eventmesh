package org.apache.eventmesh.runtime.boot;

import org.apache.eventmesh.common.adminserver.HeartBeat;
import org.apache.eventmesh.common.utils.IPUtils;
import org.apache.eventmesh.registry.QueryInstances;
import org.apache.eventmesh.registry.RegisterServerInfo;
import org.apache.eventmesh.registry.RegistryFactory;
import org.apache.eventmesh.registry.RegistryService;
import org.apache.eventmesh.runtime.Runtime;
import org.apache.eventmesh.runtime.RuntimeFactory;
import org.apache.eventmesh.runtime.RuntimeInstanceConfig;

import org.apache.eventmesh.runtime.rpc.AdminBiStreamServiceGrpc;
import org.apache.eventmesh.runtime.rpc.AdminBiStreamServiceGrpc.AdminBiStreamServiceStub;
import org.apache.eventmesh.runtime.rpc.Metadata;
import org.apache.eventmesh.runtime.rpc.Payload;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuntimeInstance {

    private String adminServerAddr;

    private Map<String, RegisterServerInfo> adminServerInfoMap = new HashMap<>();

    private ManagedChannel channel;

    private AdminBiStreamServiceStub adminStub;

    StreamObserver<Payload> responseObserver;

    StreamObserver<Payload> requestObserver;

    private final RegistryService registryService;

    private Runtime runtime;

    private RuntimeFactory runtimeFactory;

    private final RuntimeInstanceConfig runtimeInstanceConfig;

    private volatile boolean isStarted = false;

    private ScheduledExecutorService heartBeatExecutor;

    public RuntimeInstance(RuntimeInstanceConfig runtimeInstanceConfig) {
        this.runtimeInstanceConfig = runtimeInstanceConfig;
        this.registryService = RegistryFactory.getInstance(runtimeInstanceConfig.getRegistryPluginType());
    }

    public void init() {
        registryService.init();
        QueryInstances queryInstances = new QueryInstances();
        queryInstances.setServiceName(runtimeInstanceConfig.getAdminServiceName());
        queryInstances.setHealth(true);
        List<RegisterServerInfo> adminServerRegisterInfoList = registryService.selectInstances(queryInstances);
        if (!adminServerRegisterInfoList.isEmpty()) {
            adminServerAddr = getRandomAdminServerAddr(adminServerRegisterInfoList);
        } else {
            throw new RuntimeException("admin server address is empty, please check");
        }
    }

    public void start() {
        if (!StringUtils.isBlank(adminServerAddr)) {
            // create gRPC channel
            channel = ManagedChannelBuilder.forTarget(adminServerAddr)
                .usePlaintext()
                .build();

            adminStub = AdminBiStreamServiceGrpc.newStub(channel).withWaitForReady();

            responseObserver = new StreamObserver<Payload>() {
                @Override
                public void onNext(Payload response) {
                    log.info("runtime receive message: {} ", response);
                }

                @Override
                public void onError(Throwable t) {
                    log.error("runtime receive error message: {}", t.getMessage());
                }

                @Override
                public void onCompleted() {
                    log.info("runtime finished receive message and completed");
                }
            };

            requestObserver = adminStub.invokeBiStream(responseObserver);

            heartBeatExecutor = Executors.newSingleThreadScheduledExecutor();
            heartBeatExecutor.scheduleAtFixedRate(() -> {

                HeartBeat heartBeat = HeartBeat.newBuilder()
                    .setAddress(IPUtils.getLocalAddress())
                    .setReportedTimeStamp(String.valueOf(System.currentTimeMillis()))
                    .build();


                Metadata metadata = Metadata.newBuilder()
                    .setType(HeartBeat.class.getSimpleName())
                    .build();

                Payload request = Payload.newBuilder()
                    .setMetadata(metadata)
                    .setBody(Any.pack(heartBeat))
                    .build();

                requestObserver.onNext(request);
            }, 5, 5, TimeUnit.SECONDS);

            registryService.subscribe((event) -> {
                log.info("runtime receive registry event: {}", event);
                List<RegisterServerInfo> registerServerInfoList = event.getInstances();
                Map<String, RegisterServerInfo> registerServerInfoMap = new HashMap<>();
                for (RegisterServerInfo registerServerInfo : registerServerInfoList) {
                    registerServerInfoMap.put(registerServerInfo.getAddress(), registerServerInfo);
                }
                adminServerInfoMap = registerServerInfoMap;
                updateAdminServerAddr();
            }, runtimeInstanceConfig.getAdminServiceName());
            isStarted = true;
        } else {
            throw new RuntimeException("admin server address is empty, please check");
        }
    }

    public void shutdown() {
        if (heartBeatExecutor != null) {
            heartBeatExecutor.shutdown();
        }
        requestObserver.onCompleted();
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }

    private void updateAdminServerAddr() {
        if (isStarted) {
            if (!adminServerInfoMap.containsKey(adminServerAddr)) {
                adminServerAddr = getRandomAdminServerAddr(adminServerInfoMap);
                log.info("admin server address changed to: {}", adminServerAddr);
                shutdown();
                start();
            }
        } else {
            adminServerAddr = getRandomAdminServerAddr(adminServerInfoMap);
        }
    }

    private String getRandomAdminServerAddr(Map<String, RegisterServerInfo> adminServerInfoMap) {
        ArrayList<String> addresses = new ArrayList<>(adminServerInfoMap.keySet());
        Random random = new Random();
        int randomIndex = random.nextInt(addresses.size());
        return addresses.get(randomIndex);
    }

    private String getRandomAdminServerAddr(List<RegisterServerInfo> adminServerRegisterInfoList) {
        Random random = new Random();
        int randomIndex = random.nextInt(adminServerRegisterInfoList.size());
        return adminServerRegisterInfoList.get(randomIndex).getAddress();
    }

}
