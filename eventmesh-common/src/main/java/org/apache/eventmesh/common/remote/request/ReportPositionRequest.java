package org.apache.eventmesh.common.remote.request;

import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.Position;

import java.util.Map;

import lombok.Data;

@Data
public class ReportPositionRequest {

    private String jobID;

    private Position<Map<String, Object>> position;

    private JobState state;

}
