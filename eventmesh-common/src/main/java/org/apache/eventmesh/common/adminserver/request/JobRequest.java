package org.apache.eventmesh.common.adminserver.request;

import lombok.Data;

@Data
public class JobRequest extends BaseRequest {
    String jobID;
}
