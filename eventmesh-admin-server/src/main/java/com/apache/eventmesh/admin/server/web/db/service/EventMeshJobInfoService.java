package com.apache.eventmesh.admin.server.web.db.service;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobInfo;
import com.baomidou.mybatisplus.extension.service.IService;
import org.apache.eventmesh.common.remote.JobState;

/**
* @author sodafang
* @description 针对表【event_mesh_job_info】的数据库操作Service
* @createDate 2024-05-09 15:51:45
*/
public interface EventMeshJobInfoService extends IService<EventMeshJobInfo> {
    boolean updateJobState(Integer jobID, JobState state);
}
