package org.apache.eventmesh.common.remote.request;

import lombok.Data;

@Data
public class FetchJobRequest extends BaseGrpcRequest {
    String primaryKey;
}
