package org.apache.eventmesh.runtime.boot;

import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.runtime.RuntimeInstanceConfig;
import org.apache.eventmesh.runtime.rpc.AdminBiStreamServiceGrpc;
import org.apache.eventmesh.runtime.rpc.AdminBiStreamServiceGrpc.AdminBiStreamServiceStub;
import org.apache.eventmesh.runtime.rpc.Payload;
import org.apache.eventmesh.runtime.util.BannerUtil;

import java.io.File;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuntimeInstanceStarter {

    public static void main(String[] args) {
        try {
            RuntimeInstanceConfig runtimeInstanceConfig = ConfigService.getInstance().buildConfigInstance(RuntimeInstanceConfig.class);
            RuntimeInstance runtimeInstance = new RuntimeInstance(runtimeInstanceConfig);
            BannerUtil.generateBanner();
            runtimeInstance.init();
            runtimeInstance.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    log.info("runtime shutting down hook begin.");
                    long start = System.currentTimeMillis();
                    runtimeInstance.shutdown();
                    long end = System.currentTimeMillis();

                    log.info("runtime shutdown cost {}ms", end - start);
                } catch (Exception e) {
                    log.error("exception when shutdown {}", e.getMessage(), e);
                }
            }));
        } catch (Throwable e) {
            log.error("runtime start fail {}.", e.getMessage(), e);
            System.exit(-1);
        }

    }
}
