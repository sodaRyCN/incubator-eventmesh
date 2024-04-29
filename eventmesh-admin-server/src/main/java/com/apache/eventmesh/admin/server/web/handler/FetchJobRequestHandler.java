package com.apache.eventmesh.admin.server.web.handler;

import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.request.FetchJobRequest;
import org.apache.eventmesh.common.remote.response.FetchJobResponse;
import org.springframework.stereotype.Component;

@Component
public class FetchJobRequestHandler extends BaseRequestHandler<FetchJobRequest, FetchJobResponse> {



    @Override
    public FetchJobResponse handler(FetchJobRequest request, Metadata metadata) {
        FetchJobResponse response = new FetchJobResponse();
        response.setId(123);
        response.setName("demo");
        response.setSuccess(true);
        return response;
    }
}
