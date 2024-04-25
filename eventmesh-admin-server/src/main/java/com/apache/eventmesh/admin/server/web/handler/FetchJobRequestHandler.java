package com.apache.eventmesh.admin.server.web.handler;

import org.apache.eventmesh.common.adminserver.request.JobRequest;
import org.apache.eventmesh.common.adminserver.response.JobResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;

public class FetchJobRequestHandler extends BaseHandler<JobRequest, JobResponse> {

    @Override
    public JobResponse handler(JobRequest request, Metadata metadata) {
        JobResponse response = new JobResponse();
        return response;
    }
}
