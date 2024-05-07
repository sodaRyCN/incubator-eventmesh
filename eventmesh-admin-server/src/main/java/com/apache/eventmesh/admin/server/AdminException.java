package com.apache.eventmesh.admin.server;

import lombok.Getter;

public class AdminException extends RuntimeException {
    @Getter
    private final int code;
    public AdminException(int code, String message) {
        super(message);
        this.code = code;
    }
}
