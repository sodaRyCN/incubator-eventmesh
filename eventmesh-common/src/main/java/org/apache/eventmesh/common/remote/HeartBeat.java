package org.apache.eventmesh.common.remote;

import lombok.Data;

@Data
public class HeartBeat {

    private String address;

    private String reportedTimeStamp;

    private String jobID;
}
