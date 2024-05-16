package com.apache.eventmesh.admin.server.web.handler.request.impl;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.DBThreadPool;
import com.apache.eventmesh.admin.server.web.handler.position.IFetchPositionHandler;
import com.apache.eventmesh.admin.server.web.handler.position.PositionHandlerFactory;
import com.apache.eventmesh.admin.server.web.handler.request.BaseRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.FetchPositionRequest;
import org.apache.eventmesh.common.remote.response.EmptyAckResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FetchPositionHandler extends BaseRequestHandler<FetchPositionRequest, EmptyAckResponse> {

    @Autowired
    DBThreadPool executor;

    @Autowired
    PositionHandlerFactory positionHandlerFactory;

    @Override
    protected EmptyAckResponse handler(FetchPositionRequest request, Metadata metadata) {
        IFetchPositionHandler handler = positionHandlerFactory.getHandler(request.getDataSourceType());
        if (handler == null) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, String.format("illegal data source type [%s] not " +
                    "match any handler", request.getJobID()));
        }
        executor.getExecutors().execute(() -> {
            try {
                handler.handler(request, metadata);
            } catch (Exception e) {
                log.warn("");
            }
        });
        return new EmptyAckResponse();
    }
}
