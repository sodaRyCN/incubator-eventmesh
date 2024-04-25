package com.apache.eventmesh.admin.server.web.handler;

import org.apache.eventmesh.common.adminserver.request.FetchJobRequest;
import org.apache.eventmesh.common.adminserver.response.JobResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;

public class FetchJobRequestHandler extends AbstractRequestHandler<FetchJobRequest, JobResponse> {

    @Override
    public JobResponse handler(FetchJobRequest request, Metadata metadata) {
        JobResponse response = new JobResponse();
        return response;
    }
}
