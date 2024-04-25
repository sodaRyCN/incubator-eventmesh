package org.apache.eventmesh.common.adminserver.request;

import lombok.Data;

@Data
public class FetchJobByPrimaryKeyReq extends BaseRequest {
    String primaryKey;
}
