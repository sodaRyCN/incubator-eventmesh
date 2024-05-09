package org.apache.eventmesh.common.remote.request;

import lombok.Data;
import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.Position;
import org.apache.eventmesh.common.remote.offset.RecordPosition;

import java.util.List;
import java.util.Map;

@Data
public class ReportPositionRequest extends BaseGrpcRequest {

    private String jobID;

    private List<RecordPosition> recordPositionList;

    private JobState state;

}
