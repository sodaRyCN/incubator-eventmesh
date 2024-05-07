package org.apache.eventmesh.common.remote.payload;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.protobuf.Any;
import com.google.protobuf.UnsafeByteOperations;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.apache.eventmesh.common.remote.exception.ErrorCode;
import org.apache.eventmesh.common.remote.exception.PayloadFormatException;
import org.apache.eventmesh.common.remote.request.BaseGrpcRequest;
import org.apache.eventmesh.common.remote.response.BaseGrpcResponse;
import org.apache.eventmesh.common.utils.JsonUtils;

public class PayloadUtil {
    public static Payload from(BaseGrpcRequest request) {
        return null;
    }

    public static Payload from(BaseGrpcResponse response) {
        byte[] responseBytes = JsonUtils.toJSONBytes(response);
        Metadata.Builder metadata = Metadata.newBuilder().setType(response.getClass().getSimpleName());
        return Payload.newBuilder().setMetadata(metadata).setBody(Any.newBuilder().setValue(UnsafeByteOperations.unsafeWrap(responseBytes))).build();
    }

    public static BaseGrpcRequest parse(Payload payload) {
        Class<?> targetClass = PayloadFactory.getInstance().getClassByType(payload.getMetadata().getType());
        if (targetClass == null) {
            throw new PayloadFormatException(ErrorCode.BAD_REQUEST,
                    "Unknown payload type:" + payload.getMetadata().getType());
        }
        return (BaseGrpcRequest)JsonUtils.parseObject(new ByteBufferBackedInputStream(payload.getBody().getValue().asReadOnlyByteBuffer()), targetClass);
    }
}
