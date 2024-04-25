package org.apache.eventmesh.common.adminserver.request;

import lombok.Data;

@Data
public class FetchJobRequest extends BaseRequest {
    String primaryKey;
}
