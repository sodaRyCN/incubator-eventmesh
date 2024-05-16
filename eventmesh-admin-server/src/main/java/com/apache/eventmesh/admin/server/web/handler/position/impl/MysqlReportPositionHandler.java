package com.apache.eventmesh.admin.server.web.handler.position.impl;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshMysqlPosition;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshMysqlPositionService;
import com.apache.eventmesh.admin.server.web.handler.position.PositionHandler;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.job.DataSourceType;
import org.apache.eventmesh.common.remote.offset.RecordPosition;
import org.apache.eventmesh.common.remote.request.FetchPositionRequest;
import org.apache.eventmesh.common.remote.request.ReportPositionRequest;
import org.apache.eventmesh.common.remote.response.FetchPositionResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class MysqlReportPositionHandler extends PositionHandler {
    @Autowired
    EventMeshMysqlPositionService positionService;

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MYSQL;
    }

    @Override
    public boolean handler(ReportPositionRequest request, Metadata metadata) {
        for (int i = 0; i < 3; i++) {
            try {
                List<RecordPosition> recordPositionList = request.getRecordPositionList();
                EventMeshMysqlPosition position = new EventMeshMysqlPosition();
                position.setJobID(Integer.parseInt(request.getJobID()));
                position.setAddress(request.getAddress());

                if (!positionService.saveOrUpdateByJob(position)) {
                    log.warn("update job position fail [{}]", request);
                    return false;
                }
                return true;
            } catch (DuplicateKeyException e) {
                log.warn("concurrent report position job [{}], it will try again", request.getJobID());
            } catch (Exception e) {
                log.warn("save position job [{}] fail", request.getJobID(), e);
                return false;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ignore) {
                log.warn("save position thread interrupted, [{}]", request);
                return true;
            }
        }
        return false;
    }

    @Override
    public FetchPositionResponse handler(FetchPositionRequest request, Metadata metadata) {
        try {
            EventMeshMysqlPosition position = positionService.getOne(Wrappers.<EventMeshMysqlPosition>query().eq("jobID"
                    , request.getJobID()));
            FetchPositionResponse response = FetchPositionResponse.successResponse();
            return response;
        } catch (Exception e) {
            return FetchPositionResponse.failResponse(ErrorCode.INTERNAL_ERR,"");
        }
    }
}
