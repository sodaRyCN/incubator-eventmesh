package com.apache.eventmesh.admin.server.web.handler.request.impl;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.DBThreadPool;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshDataSourceService;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobInfoService;
import com.apache.eventmesh.admin.server.web.handler.position.IReportPositionHandler;
import com.apache.eventmesh.admin.server.web.handler.position.PositionHandlerFactory;
import com.apache.eventmesh.admin.server.web.handler.request.BaseRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.ReportPositionRequest;
import org.apache.eventmesh.common.remote.response.EmptyAckResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReportPositionHandler extends BaseRequestHandler<ReportPositionRequest, EmptyAckResponse> {
    @Autowired
    EventMeshJobInfoService jobInfoService;

    @Autowired
    EventMeshDataSourceService dataSourceService;

    @Autowired
    DBThreadPool executor;

    @Autowired
    PositionHandlerFactory positionHandlerFactory;


    @Override
    protected EmptyAckResponse handler(ReportPositionRequest request, Metadata metadata) {
        if (request.getDataSourceType() == null) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, "illegal data type, it's empty");
        }
        if (StringUtils.isBlank(request.getJobID())) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, "illegal job id, it's empty");
        }
        if (request.getRecordPositionList() == null || request.getRecordPositionList().isEmpty()) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, "illegal record position list, it's empty");
        }
        int jobID;

        try {
            jobID = Integer.parseInt(request.getJobID());
        } catch (NumberFormatException e) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, String.format("illegal job id [%s] format",
                    request.getJobID()));
        }

        IReportPositionHandler handler = positionHandlerFactory.getHandler(request.getDataSourceType());
        if (handler == null) {
            throw new AdminServerException(ErrorCode.BAD_DB_DATA, String.format("illegal data base job id [%s] " +
                            "type [%s], it not match any handler", request.getJobID(),
                    request.getDataSourceType().getName()));
        }

        executor.getExecutors().execute(() -> {
            try {
                jobInfoService.updateJobState(jobID, request.getState());
            } catch (Exception e) {
                log.warn("update job id [{}] type [{}] state [{}] fail", request.getJobID(),
                        request.getDataSourceType(), request.getState(), e);
            }
            try {
                handler.handler(request, metadata);
            } catch (Exception e) {
                log.warn("handle position request fail, request [{}]", request, e);
            }
        });
        return new EmptyAckResponse();
    }
}
