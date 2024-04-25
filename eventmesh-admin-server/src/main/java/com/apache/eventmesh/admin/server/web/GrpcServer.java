package com.apache.eventmesh.admin.server.web;

import com.apache.eventmesh.admin.server.AdminServerProperties;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import java.util.concurrent.TimeUnit;

@Controller
@Slf4j
public class GrpcServer extends BaseServer {

    @Autowired
    AdminGrpcServer adminGrpcServer;

    @Autowired
    AdminServerProperties properties;

    private Server server;

    @Override
    public void start() throws Exception {
        NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(getPort()).addService(adminGrpcServer);
        if (properties.isEnable()) {
            serverBuilder.sslContext(null);
        }
        server = serverBuilder.build();
        server.start();
    }

    @Override
    public void destroy() {
        try {
            if (server != null) {
                server.shutdown();
                server.awaitTermination(30, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            log.warn("destroy [{}] server fail", this.getClass().getSimpleName(), e);
        }
    }

    @Override
    public int getPort() {
        return properties.getPort();
    }
}
