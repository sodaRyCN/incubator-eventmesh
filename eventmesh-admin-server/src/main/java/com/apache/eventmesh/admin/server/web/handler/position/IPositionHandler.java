package com.apache.eventmesh.admin.server.web.handler.position;

import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.remote.request.ReportPositionRequest;

public interface IPositionHandler {
    boolean handler(ReportPositionRequest request, Metadata metadata);
}
