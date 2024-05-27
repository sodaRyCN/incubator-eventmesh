package com.apache.eventmesh.admin.server.web.handler.request.impl;

import com.apache.eventmesh.admin.server.AdminServerException;
import com.apache.eventmesh.admin.server.web.db.DBThreadPool;
import com.apache.eventmesh.admin.server.web.service.position.IFetchPositionHandler;
import com.apache.eventmesh.admin.server.web.service.position.PositionHandlerFactory;
import com.apache.eventmesh.admin.server.web.handler.request.BaseRequestHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.request.FetchPositionRequest;
import org.apache.eventmesh.common.remote.response.FetchPositionResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class FetchPositionHandler extends BaseRequestHandler<FetchPositionRequest, FetchPositionResponse> {

    @Autowired
    DBThreadPool executor;

    @Autowired
    PositionHandlerFactory positionHandlerFactory;

    @Override
    protected FetchPositionResponse handler(FetchPositionRequest request, Metadata metadata) {
        IFetchPositionHandler handler = positionHandlerFactory.getHandler(request.getDataSourceType());
        if (handler == null) {
            throw new AdminServerException(ErrorCode.BAD_REQUEST, String.format("illegal data source type [%s] not " +
                    "match any handler", request.getJobID()));
        }

        try {
            return FetchPositionResponse.successResponse(handler.handler(request, metadata));
        } catch (Exception e) {
            log.warn("fetch request [{}] position fail", request, e);
            return FetchPositionResponse.failResponse(ErrorCode.INTERNAL_ERR, e.getMessage());
        }
    }
}
