package org.apache.eventmesh.common.adminserver.response;

import lombok.Data;
import org.apache.eventmesh.common.adminserver.IPayload;

@Data
public abstract class BaseResponse implements IPayload {
    public static final int UNKNOWN = -1;
    private boolean success = true;
    private int errorCode;
    private String desc;
}
