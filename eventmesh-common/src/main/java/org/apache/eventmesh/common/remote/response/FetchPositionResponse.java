package org.apache.eventmesh.common.remote.response;

import org.apache.eventmesh.common.remote.JobState;
import org.apache.eventmesh.common.remote.Position;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.job.JobTransportType;
import org.apache.eventmesh.common.remote.offset.RecordPosition;

import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class FetchPositionResponse extends BaseGrpcResponse {

    private RecordPosition recordPosition;

    public static FetchPositionResponse successResponse() {
        FetchPositionResponse response = new FetchPositionResponse();
        response.setSuccess(true);
        response.setErrorCode(ErrorCode.SUCCESS);
        return response;
    }

    public static FetchPositionResponse failResponse(int code, String desc) {
        FetchPositionResponse response = new FetchPositionResponse();
        response.setSuccess(false);
        response.setErrorCode(code);
        response.setDesc(desc);
        return response;
    }

}
