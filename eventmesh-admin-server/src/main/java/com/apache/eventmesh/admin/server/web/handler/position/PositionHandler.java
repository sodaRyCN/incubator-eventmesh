package com.apache.eventmesh.admin.server.web.handler.position;

import org.apache.eventmesh.common.remote.job.DataSourceType;

public abstract class PositionHandler implements IPositionHandler {
    protected abstract DataSourceType getSourceType();
}
