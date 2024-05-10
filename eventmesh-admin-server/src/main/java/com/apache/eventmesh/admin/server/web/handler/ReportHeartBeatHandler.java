package com.apache.eventmesh.admin.server.web.handler;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshHeartbeat;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshHeartbeatService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.ReportHeartBeatRequest;
import org.apache.eventmesh.common.remote.response.EmptyAckResponse;
import org.apache.eventmesh.common.utils.IPUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReportHeartBeatHandler extends BaseRequestHandler<ReportHeartBeatRequest, EmptyAckResponse>{
    @Autowired
    EventMeshHeartbeatService heartbeatService;

    @Override
    protected EmptyAckResponse handler(ReportHeartBeatRequest request, Metadata metadata) {
        EventMeshHeartbeat heartbeat = new EventMeshHeartbeat();
        Integer job;
        try {
            job = Integer.parseInt(request.getJobID());
            heartbeat.setJobID(job);
        } catch (NumberFormatException e) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, String.format("illegal job id %s", request.getJobID()));
        }

        heartbeat.setReportTime(request.getReportedTimeStamp());
        heartbeat.setAdminAddr(IPUtils.getLocalAddress());
        heartbeat.setRuntimeAddr(request.getAddress());
        heartbeat.setUpdateTime(String.valueOf(System.currentTimeMillis()));
        UpdateWrapper<EventMeshHeartbeat> updateWrapper = new UpdateWrapper<>();
        QueryWrapper<EventMeshHeartbeat> queryWrapper = new QueryWrapper<>();
        for (int i = 0; i < 3; i++) {
            try {
                EventMeshHeartbeat old = heartbeatService.getOne(queryWrapper.eq("jobID", job));
                if (old == null) {
                    heartbeatService.save(heartbeat);
                } else {
                    if (Long.parseLong(old.getReportTime()) > Long.parseLong(request.getReportedTimeStamp())) {
                        break;
                    }
                    heartbeatService.update(updateWrapper.eq("updateTime", old.getUpdateTime()).setEntity(heartbeat));
                }
            } catch (DuplicateKeyException e) {
                log.warn("concurrent insert heart beat record, job id [{}], runtime [{}]", job, request.getAddress());
            }
        }
        return new EmptyAckResponse();
    }
}
