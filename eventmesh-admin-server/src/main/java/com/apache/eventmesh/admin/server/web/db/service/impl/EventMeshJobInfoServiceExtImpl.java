package com.apache.eventmesh.admin.server.web.db.service.impl;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobDetail;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshJobInfo;
import com.apache.eventmesh.admin.server.web.db.mapper.EventMeshJobInfoMapper;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshJobInfoExtService;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
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
public class EventMeshJobInfoServiceExtImpl extends ServiceImpl<EventMeshJobInfoMapper, EventMeshJobInfo>
    implements EventMeshJobInfoExtService {

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

    @Override
    public EventMeshJobDetail getJobDetail(Integer jobID) {
        return null;
    }
}




