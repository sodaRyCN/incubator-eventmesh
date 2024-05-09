package com.apache.eventmesh.admin.server;

import lombok.Getter;

public class AdminServerException extends RuntimeException {
    @Getter
    private final int code;
    public AdminServerException(int code, String message) {
        super(message);
        this.code = code;
    }
}
