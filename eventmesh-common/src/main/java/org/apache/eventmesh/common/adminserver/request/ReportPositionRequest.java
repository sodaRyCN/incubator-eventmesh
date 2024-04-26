package org.apache.eventmesh.common.adminserver.request;

import org.apache.eventmesh.common.adminserver.JobState;
import org.apache.eventmesh.common.adminserver.Position;

import lombok.Data;

@Data
public class ReportPositionRequest {

    private String jobID;

    private Position position;

    private JobState state;

}
