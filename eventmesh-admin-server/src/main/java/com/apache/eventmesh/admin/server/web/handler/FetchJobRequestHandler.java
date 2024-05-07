package com.apache.eventmesh.admin.server.web.handler;

import com.apache.eventmesh.admin.server.AdminException;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobInfo;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobInfoService;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.FetchJobRequest;
import org.apache.eventmesh.common.remote.response.FetchJobResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class FetchJobRequestHandler extends BaseRequestHandler<FetchJobRequest, FetchJobResponse> {

    @Autowired
    EventMeshJobInfoService eventMeshJobInfoService;


    @Override
    public FetchJobResponse handler(FetchJobRequest request, Metadata metadata) {

        if (StringUtils.isBlank(request.getJobID())) {
            return FetchJobResponse.failResponse(ErrorCode.BAD_REQUEST, "job id is empty");
        }
        FetchJobResponse response = FetchJobResponse.successResponse();
        EventMeshJobInfo job = eventMeshJobInfoService.getById(request.getJobID());
        response.setId(job.getJobID());
        response.setName("demo");
        response.setSinkConnectorConfig(null);
        response.setSinkConnectorDesc("sink desc");
        response.setSourceConnectorConfig(null);
        response.setSourceConnectorDesc("source desc");
        response.setPosition(null);
        JobState state = JobState.fromIndex(job.getState());
        if (state == null) {
            throw new AdminException(ErrorCode.BAD_DB_DATA,"illegal job state in db");
        }
        response.setState(state);
//        response.setTransportType(job.getTransportType());
        return response;
    }
}
