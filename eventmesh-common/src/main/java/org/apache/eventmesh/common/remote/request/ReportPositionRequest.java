package org.apache.eventmesh.common.remote.request;

import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.Position;

import lombok.Data;

@Data
public class ReportPositionRequest {

    private String jobID;

    private Position position;

    private JobState state;

}
