package com.apache.eventmesh.admin.server.web.handler.request;

import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.request.BaseGrpcRequest;
import org.apache.eventmesh.common.remote.response.BaseGrpcResponse;

public abstract class BaseRequestHandler<T extends BaseGrpcRequest, S extends BaseGrpcResponse> {
    public BaseGrpcResponse handlerRequest(T request, Metadata metadata) {
        return handler(request, metadata);
    }

    protected abstract S handler(T request, Metadata metadata);
}
