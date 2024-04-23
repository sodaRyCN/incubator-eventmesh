package com.apache.eventmesh.admin.server.web;

import com.apache.eventmesh.admin.server.ComponentLifeCycle;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminBiStreamServiceGrpc;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;

@Controller
public class GrpcServer extends AdminBiStreamServiceGrpc.AdminBiStreamServiceImplBase implements ComponentLifeCycle {

    @PostConstruct
    @Override
    public void start() {

    }

    @Override
    public void destroy() {

    }
}
