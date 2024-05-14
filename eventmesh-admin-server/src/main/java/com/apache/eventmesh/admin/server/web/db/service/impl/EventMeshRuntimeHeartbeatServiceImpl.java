package com.apache.eventmesh.admin.server.web.db.service.impl;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshRuntimeHistory;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshRuntimeHistoryService;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshRuntimeHeartbeat;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshRuntimeHeartbeatService;
import com.apache.eventmesh.admin.server.web.db.mapper.EventMeshRuntimeHeartbeatMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
* @author sodafang
* @description 针对表【event_mesh_runtime_heartbeat】的数据库操作Service实现
* @createDate 2024-05-14 17:15:03
*/
@Service
@Slf4j
public class EventMeshRuntimeHeartbeatServiceImpl extends ServiceImpl<EventMeshRuntimeHeartbeatMapper, EventMeshRuntimeHeartbeat>
    implements EventMeshRuntimeHeartbeatService{

    @Autowired
    EventMeshRuntimeHistoryService historyService;

    @Override
    public boolean saveOrUpdateByRuntimeAddress(EventMeshRuntimeHeartbeat entity) {
        EventMeshRuntimeHeartbeat old = getOne(Wrappers.<EventMeshRuntimeHeartbeat>query().eq("runtimeAddr",
                entity.getRuntimeAddr()));
        if (old == null) {
            return save(entity);
        } else {
            if (old.getJobID() != null && !old.getJobID().equals(entity.getJobID())) {
                EventMeshRuntimeHistory history = new EventMeshRuntimeHistory();
                history.setAddress(old.getAdminAddr());
                history.setJob(old.getJobID());
                try {
                    historyService.save(history);
                } catch (Exception e) {
                    log.warn("save runtime job changed history fail", e);
                }

                log.info("runtime [{}] changed job, old job [{}], now [{}]",entity.getRuntimeAddr(),old.getJobID(),
                        entity.getJobID());
            }
            if (Long.parseLong(old.getReportTime()) > Long.parseLong(entity.getReportTime())) {
                log.info("update heartbeat record ignore, current report time late than db, job " +
                        "[{}], remote [{}]", entity.getJobID(), entity.getRuntimeAddr());
                return true;
            }
            return update(entity, Wrappers.<EventMeshRuntimeHeartbeat>update().eq("updateTime", old.getUpdateTime()));
        }
    }
}




