package com.apache.eventmesh.admin.server.web;

import com.apache.eventmesh.admin.server.web.handler.RequestHandlerFactory;
import com.google.protobuf.Any;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.adminserver.response.BaseResponse;
import org.apache.eventmesh.common.adminserver.response.FailResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AdminGrpcServer extends AdminServiceGrpc.AdminServiceImplBase {
    @Autowired
    RequestHandlerFactory handlerFactory;

    public StreamObserver<Payload> invokeBiStream(StreamObserver<Payload> responseObserver) {
        return new StreamObserver<Payload>() {
            @Override
            public void onNext(Payload value) {
                if (value == null || StringUtils.isBlank(value.getMetadata().getType())) {
                    responseObserver.onNext(Payload.newBuilder().setBody(Any.newBuilder().setValue(UnsafeByteOperations.unsafeWrap(JsonUtils.toJSONBytes(FailResponse.build(BaseResponse.UNKNOWN, "bad " +
                            "request"))))).build());
                    return;
                }
                handlerFactory.getHandler(value.getMetadata().getType());
                responseObserver.onNext();
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        };
    }

    public void invoke(Payload request, StreamObserver<Payload> responseObserver) {
    }
}
