package com.apache.eventmesh.admin.server.web.db.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobInfo;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobInfoService;
import com.apache.eventmesh.admin.server.web.db.mapper.EventMeshJobInfoMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.remote.JobState;
import org.springframework.stereotype.Service;

/**
* @author sodafang
* @description 针对表【event_mesh_job_info】的数据库操作Service实现
* @createDate 2024-05-09 15:51:45
*/
@Service
@Slf4j
public class EventMeshJobInfoServiceImpl extends ServiceImpl<EventMeshJobInfoMapper, EventMeshJobInfo>
    implements EventMeshJobInfoService{

    @Override
    public boolean updateJobState(Integer jobID, JobState state) {
        if (jobID == null || state == null) {
            return false;
        }
        EventMeshJobInfo jobInfo = new EventMeshJobInfo();
        jobInfo.setJobID(jobID);
        jobInfo.setState(state.ordinal());
        update(jobInfo, Wrappers.<EventMeshJobInfo>update().notIn("state",JobState.DELETE.ordinal(),
                JobState.COMPLETE.ordinal()));
        return true;
    }
}




