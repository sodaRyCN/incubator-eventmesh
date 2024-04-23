package org.apache.eventmesh.registry;

import lombok.Getter;

import java.util.List;

public class NotifyEvent {
    // means whether it is an increment data
    @Getter
    private boolean isIncrement = false;

    @Getter
    private List<RegisterServerInfo> instances;
}
