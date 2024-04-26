package org.apache.eventmesh.common.adminserver.response;

import org.apache.eventmesh.common.adminserver.JobState;
import org.apache.eventmesh.common.adminserver.Position;
import org.apache.eventmesh.common.adminserver.job.JobTransportType;

import java.util.Map;

import lombok.Data;

@Data
public class FetchJobResponse extends BaseResponse {

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
