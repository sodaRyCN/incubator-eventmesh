package com.apache.eventmesh.admin.server.web;

import com.apache.eventmesh.admin.server.web.handler.BaseRequestHandler;
import com.apache.eventmesh.admin.server.web.handler.RequestHandlerFactory;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.remote.payload.PayloadUtil;
import org.apache.eventmesh.common.remote.request.BaseGrpcRequest;
import org.apache.eventmesh.common.remote.response.BaseGrpcResponse;
import org.apache.eventmesh.common.remote.response.FailResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AdminGrpcServer extends AdminServiceGrpc.AdminServiceImplBase {
    @Autowired
    RequestHandlerFactory handlerFactory;

    private Payload process(Payload value) {
        if (value == null || StringUtils.isBlank(value.getMetadata().getType())) {
            return PayloadUtil.from(FailResponse.build(BaseGrpcResponse.UNKNOWN, "bad request"));
        }
        BaseRequestHandler<BaseGrpcRequest, BaseGrpcResponse> handler =
                handlerFactory.getHandler(value.getMetadata().getType());
        if (handler == null) {
            return PayloadUtil.from(FailResponse.build(BaseGrpcResponse.UNKNOWN,
                    "not match any request handler"));
        }
        return PayloadUtil.from(handler.handlerRequest(PayloadUtil.parse(value), value.getMetadata()));
    }

    public StreamObserver<Payload> invokeBiStream(StreamObserver<Payload> responseObserver) {
        return new StreamObserver<Payload>() {
            @Override
            public void onNext(Payload value) {
                responseObserver.onNext(process(value));
            }

            @Override
            public void onError(Throwable t) {
                if (responseObserver instanceof ServerCallStreamObserver) {
                    if (!((ServerCallStreamObserver<Payload>) responseObserver).isCancelled()) {
                        log.warn("admin gRPC server fail", t);
                    }
                }
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    public void invoke(Payload request, StreamObserver<Payload> responseObserver) {
        responseObserver.onNext(process(request));
        responseObserver.onCompleted();
    }
}
