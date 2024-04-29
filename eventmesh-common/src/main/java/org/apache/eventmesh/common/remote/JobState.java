package org.apache.eventmesh.common.remote;

public enum JobState {
    INIT,
    STARTED,
    RUNNING,
    PAUSE,
    COMPLETE,
    DELETE,
    FAIL
}
