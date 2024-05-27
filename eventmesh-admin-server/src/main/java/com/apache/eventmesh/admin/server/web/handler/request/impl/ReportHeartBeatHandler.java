package com.apache.eventmesh.admin.server.web.handler.request.impl;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.DBThreadPool;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshRuntimeHeartbeat;
import com.apache.eventmesh.admin.server.web.handler.request.BaseRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.ReportHeartBeatRequest;
import org.apache.eventmesh.common.remote.response.EmptyAckResponse;
import org.apache.eventmesh.common.utils.IPUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ReportHeartBeatHandler extends BaseRequestHandler<ReportHeartBeatRequest, EmptyAckResponse> {
    @Autowired
    EventMeshRuntimeHeartbeatExtService heartbeatExtService;

    @Autowired
    DBThreadPool executor;

    @Override
    protected EmptyAckResponse handler(ReportHeartBeatRequest request, Metadata metadata) {
        executor.getExecutors().execute(() -> {
            EventMeshRuntimeHeartbeat heartbeat = new EventMeshRuntimeHeartbeat();
            int job;
            try {
                job = Integer.parseInt(request.getJobID());
            } catch (NumberFormatException e) {
                throw new AdminServerException(ErrorCode.BAD_REQUEST, String.format("illegal job id %s",
                        request.getJobID()));
            }
            heartbeat.setJobID(job);
            heartbeat.setReportTime(request.getReportedTimeStamp());
            heartbeat.setAdminAddr(IPUtils.getLocalAddress());
            heartbeat.setRuntimeAddr(request.getAddress());
            try {
                if (!heartbeatExtService.saveOrUpdateByRuntimeAddress(heartbeat)) {
                    log.warn("save or update heartbeat request [{}] fail", request);
                }
            } catch (Exception e) {
                log.warn("save or update heartbeat request [{}] fail", request, e);
            }
        });

        return new EmptyAckResponse();
    }
}
