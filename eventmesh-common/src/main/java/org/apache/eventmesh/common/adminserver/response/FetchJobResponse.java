package org.apache.eventmesh.common.adminserver.response;

import org.apache.eventmesh.common.adminserver.job.JobTransportType;

import lombok.Data;

@Data
public class FetchJobResponse extends BaseResponse {

    private long id;
    private String name;
    private String desc;
    private String sourceUser;
    private String sourcePasswd;
    private String targetUser;
    private String targetPasswd;
    private JobTransportType transportType;
}
