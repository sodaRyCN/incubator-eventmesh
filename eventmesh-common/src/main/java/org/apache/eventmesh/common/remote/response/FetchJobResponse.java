package org.apache.eventmesh.common.remote.response;

import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.Position;
import org.apache.eventmesh.common.remote.job.JobTransportType;

import java.util.Map;

import lombok.Data;
import org.apache.eventmesh.common.remote.response.BaseGrpcResponse;

@Data
public class FetchJobResponse extends BaseGrpcResponse {

    private long id;

    private String name;

    private JobTransportType transportType;

    private Map<String, Object> sourceConnectorConfig;

    private String sourceConnectorDesc;

    private Map<String, Object> sinkConnectorConfig;

    private String sinkConnectorDesc;

    private Position position;

    private JobState state;

}
