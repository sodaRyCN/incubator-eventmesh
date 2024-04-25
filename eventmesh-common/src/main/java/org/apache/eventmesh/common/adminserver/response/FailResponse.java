package org.apache.eventmesh.common.adminserver.response;

public class FailResponse extends BaseResponse {
    public static BaseResponse build(int errorCode, String msg) {
        FailResponse response = new FailResponse();
        response.setErrorCode(errorCode);
        response.setDesc(msg);
        response.setSuccess(false);
        return response;
    }


    /**
     * build an error response.
     *
     * @param exception exception
     * @return response
     */
    public static BaseResponse build(Throwable exception) {
        return build(BaseResponse.UNKNOWN, exception.getMessage());
    }
}
