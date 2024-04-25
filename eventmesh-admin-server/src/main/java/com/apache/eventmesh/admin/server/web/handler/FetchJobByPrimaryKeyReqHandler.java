package com.apache.eventmesh.admin.server.web.handler;

import org.apache.eventmesh.common.adminserver.request.FetchJobByPrimaryKeyReq;
import org.apache.eventmesh.common.adminserver.response.JobDetailsResponse;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;

public class FetchJobByPrimaryKeyReqHandler extends BaseHandler<FetchJobByPrimaryKeyReq, JobDetailsResponse> {

    @Override
    public JobDetailsResponse handler(FetchJobByPrimaryKeyReq request, Metadata metadata) {
        JobDetailsResponse response = new JobDetailsResponse();
        return response;
    }
}
