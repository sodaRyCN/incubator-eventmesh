package com.apache.eventmesh.admin.server.web.handler;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshDataSource;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobInfo;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshDataSourceService;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobInfoService;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobPositionService;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.job.JobTransportType;
import org.apache.eventmesh.common.remote.request.FetchJobRequest;
import org.apache.eventmesh.common.remote.response.FetchJobResponse;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@Slf4j
public class FetchJobRequestHandler extends BaseRequestHandler<FetchJobRequest, FetchJobResponse> {

    @Autowired
    EventMeshJobInfoService jobInfoService;

    @Autowired
    EventMeshDataSourceService dataSourceService;

    @Autowired
    EventMeshJobPositionService positionService;


    @Override
    public FetchJobResponse handler(FetchJobRequest request, Metadata metadata) {
        if (StringUtils.isBlank(request.getJobID())) {
            return FetchJobResponse.failResponse(ErrorCode.BAD_REQUEST, "job id is empty");
        }
        FetchJobResponse response = FetchJobResponse.successResponse();
        EventMeshJobInfo job = jobInfoService.getById(request.getJobID());
        if (job == null) {
            return response;
        }
        response.setId(job.getJobID());
        response.setName(job.getName());
        EventMeshDataSource source = dataSourceService.getById(job.getSourceData());
        EventMeshDataSource target = dataSourceService.getById(job.getTargetData());
        if (source != null) {
            if (!StringUtils.isBlank(source.getConfiguration())) {
                try {
                    response.setSourceConnectorConfig(JsonUtils.parseTypeReferenceObject(source.getConfiguration(),
                            new TypeReference<Map<String, Object>>() {}));
                } catch (Exception e) {
                    log.warn("parse source config id [{}] fail", job.getSourceData(), e);
                    throw new AdminServerException(ErrorCode.BAD_DB_DATA,"illegal source data source config");
                }
            }
            response.setSourceConnectorDesc(source.getDescription());
        }
        if (target != null) {
            if (!StringUtils.isBlank(target.getConfiguration())) {
                try {
                    response.setSinkConnectorConfig(JsonUtils.parseTypeReferenceObject(target.getConfiguration(),
                            new TypeReference<Map<String, Object>>() {}));
                } catch (Exception e) {
                    log.warn("parse sink config id [{}] fail", job.getSourceData(), e);
                    throw new AdminServerException(ErrorCode.BAD_DB_DATA,"illegal target data sink config");
                }
            }
            response.setSinkConnectorDesc(target.getDescription());
        }
        response.setPosition(null);
        JobState state = JobState.fromIndex(job.getState());
        if (state == null) {
            throw new AdminServerException(ErrorCode.BAD_DB_DATA,"illegal job state in db");
        }
        response.setState(state);
        response.setTransportType(JobTransportType.getJobTransportType(job.getTransportType()));
        return response;
    }
}
