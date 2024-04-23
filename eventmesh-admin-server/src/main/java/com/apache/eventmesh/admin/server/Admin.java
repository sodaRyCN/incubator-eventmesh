package com.apache.eventmesh.admin.server;

import org.apache.eventmesh.common.adminserver.HeartBeat;
import org.apache.eventmesh.common.utils.PagedList;

import org.apache.eventmesh.common.adminserver.Task;

public interface Admin extends ComponentLifeCycle {
    /**
     * support for web or ops
     **/
    boolean createOrUpdateTask(Task task);
    boolean deleteTask(Long id);
    Task getTask(Long id);
    // paged list
    PagedList<Task> getTaskPaged(Task task);

    /**
     * support for task
     */
    void reportHeartbeat(HeartBeat heartBeat);



}
