package com.apache.eventmesh.admin.server.web.handler.position;

import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.request.FetchPositionRequest;
import org.apache.eventmesh.common.remote.response.FetchPositionResponse;

public interface IFetchPositionHandler {
    FetchPositionResponse handler(FetchPositionRequest request, Metadata metadata);
}
