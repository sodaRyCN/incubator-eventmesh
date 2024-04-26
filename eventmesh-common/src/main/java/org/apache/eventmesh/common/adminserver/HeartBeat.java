package org.apache.eventmesh.common.adminserver;

import lombok.Data;

@Data
public class HeartBeat {

    private String address;

    private String reportedTimeStamp;

    private String jobID;
}
