package com.apache.eventmesh.admin.server.web.handler;

import org.apache.eventmesh.common.adminserver.request.FetchJobRequest;
import org.apache.eventmesh.common.adminserver.response.FetchJobResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;

public class FetchJobRequestHandler extends AbstractRequestHandler<FetchJobRequest, FetchJobResponse> {

    @Override
    public FetchJobResponse handler(FetchJobRequest request, Metadata metadata) {
        FetchJobResponse response = new FetchJobResponse();
        return response;
    }
}
