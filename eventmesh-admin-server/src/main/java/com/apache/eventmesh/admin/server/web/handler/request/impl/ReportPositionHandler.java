package com.apache.eventmesh.admin.server.web.handler.request.impl;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.DBThreadPool;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshDataSource;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobInfo;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshDataSourceService;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobInfoService;
import com.apache.eventmesh.admin.server.web.handler.position.IPositionHandler;
import com.apache.eventmesh.admin.server.web.handler.position.PositionHandlerFactory;
import com.apache.eventmesh.admin.server.web.handler.request.BaseRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.job.DataSourceType;
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
        EventMeshJobInfo jobInfo = jobInfoService.getById(request.getJobID());
        if (jobInfo == null) {
            throw new AdminServerException(ErrorCode.BAD_DB_DATA, String.format("job id [%s] not exists in db",
                    request.getJobID()));
        }
        EventMeshDataSource sourceDB = dataSourceService.getById(jobInfo.getSourceData());
        if (sourceDB == null) {
            throw new AdminServerException(ErrorCode.BAD_DB_DATA, String.format("data base [%s] job id [%s] not " +
                    "exists in db", jobInfo.getSourceData(), jobInfo.getJobID()));
        }
        DataSourceType type = DataSourceType.getDataSourceType(sourceDB.getDataType());
        if (type == null) {
            throw new AdminServerException(ErrorCode.BAD_DB_DATA, String.format("illegal data base [%s] job id [%s] " +
                    "type [%d]", jobInfo.getSourceData(), jobInfo.getJobID(), sourceDB.getDataType()));
        }
        IPositionHandler handler = positionHandlerFactory.getHandler(type);
        if (handler == null) {
            throw new AdminServerException(ErrorCode.BAD_DB_DATA, String.format("illegal data base [%s] job id [%s] " +
                            "type [%d], it not match any handler", jobInfo.getSourceData(), jobInfo.getJobID(),
                    sourceDB.getDataType()));
        }

        executor.getExecutors().execute(() -> {
            try {
                handler.handler(request, metadata);
            } catch (Exception e) {
                log.warn("handle position request fail data base [{}] job id [{}] type [{}]", jobInfo.getSourceData()
                        , jobInfo.getJobID(), sourceDB.getDataType(), e);
            }
        });
        return new EmptyAckResponse();
    }
}
