package com.apache.eventmesh.admin.server.web;

import io.grpc.stub.StreamObserver;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.springframework.stereotype.Service;

@Service
public class AdminGrpcServer extends AdminServiceGrpc.AdminServiceImplBase {
    public StreamObserver<Payload> invokeBiStream(StreamObserver<Payload> responseObserver) {
        return null;
    }

    public void invoke(Payload request, StreamObserver<Payload> responseObserver) {

    }
}
