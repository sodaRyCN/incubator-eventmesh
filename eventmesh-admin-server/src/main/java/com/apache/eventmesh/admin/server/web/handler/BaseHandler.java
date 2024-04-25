package com.apache.eventmesh.admin.server.web.handler;

import org.apache.eventmesh.common.adminserver.request.BaseRequest;
import org.apache.eventmesh.common.adminserver.response.BaseResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;

public abstract class BaseHandler<T extends BaseRequest, S extends BaseResponse> {
    public BaseResponse handlerRequest(T request, Metadata metadata) {
        return handler(request, metadata);
    }

    public abstract S handler(T request, Metadata metadata);
}
