package com.apache.eventmesh.admin.server.web.handler.request.impl;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobDetail;
import com.apache.eventmesh.admin.server.web.handler.request.BaseRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.FetchJobRequest;
import org.apache.eventmesh.common.remote.response.FetchJobResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FetchJobRequestHandler extends BaseRequestHandler<FetchJobRequest, FetchJobResponse> {

    @Autowired
    EventMeshJobInfoExtService jobInfoExtService;




    @Override
    public FetchJobResponse handler(FetchJobRequest request, Metadata metadata) {
        if (StringUtils.isBlank(request.getJobID())) {
            return FetchJobResponse.failResponse(ErrorCode.BAD_REQUEST, "job id is empty");
        }
        int jobID;
        try {
            jobID = Integer.parseInt(request.getJobID());
        } catch (NumberFormatException e) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, String.format("illegal job id %s",
                    request.getJobID()));
        }
        FetchJobResponse response = FetchJobResponse.successResponse();
        EventMeshJobDetail detail = jobInfoExtService.getJobDetail(jobID);
        if (detail == null) {
            return response;
        }
        response.setId(detail.getId());
        response.setName(detail.getName());
        response.setSourceConnectorConfig(detail.getSourceConnectorConfig());
        response.setSourceConnectorDesc(detail.getSourceConnectorDesc());
        response.setTransportType(detail.getTransportType());
        response.setSinkConnectorConfig(detail.getSinkConnectorConfig());
        response.setSourceConnectorDesc(detail.getSinkConnectorDesc());
        response.setState(detail.getState());
        response.setPosition(detail.getPosition());
        return response;
    }
}
