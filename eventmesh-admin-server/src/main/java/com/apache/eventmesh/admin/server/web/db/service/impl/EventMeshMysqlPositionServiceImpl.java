package com.apache.eventmesh.admin.server.web.db.service.impl;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshMysqlPosition;
import com.apache.eventmesh.admin.server.web.db.entity.EventMeshPositionReporterHistory;
import com.apache.eventmesh.admin.server.web.db.mapper.EventMeshMysqlPositionMapper;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshMysqlPositionService;
import com.apache.eventmesh.admin.server.web.db.service.EventMeshPositionReporterHistoryService;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
* @author sodafang
* @description 针对表【event_mesh_mysql_position】的数据库操作Service实现
* @createDate 2024-05-14 17:15:03
*/
@Service
@Slf4j
public class EventMeshMysqlPositionServiceImpl extends ServiceImpl<EventMeshMysqlPositionMapper, EventMeshMysqlPosition>
    implements EventMeshMysqlPositionService{

    @Autowired
    EventMeshPositionReporterHistoryService historyService;

    @Override
    public boolean saveOrUpdateByJob(EventMeshMysqlPosition position) {
        EventMeshMysqlPosition old = getOne(Wrappers.<EventMeshMysqlPosition>query().eq("jobId", position.getJobID()));
        if (old == null) {
            return save(position);
        } else {
            if (old.getPosition() >= position.getPosition()) {
                log.info("job [{}] report position [{}] less than db [{}]", position.getJobID(), position.getPosition(),
                        old.getPosition());
                return true;
            }
            try {
                return update(position, Wrappers.<EventMeshMysqlPosition>update().eq("updateTime", old.getUpdateTime()));
            } finally {
                if (old.getAddress()!= null && !old.getAddress().equals(position.getAddress())) {
                    EventMeshPositionReporterHistory history = new EventMeshPositionReporterHistory();
                    history.setRecord(JsonUtils.toJSONString(position));
                    history.setJob(old.getJobID());
                    history.setAddress(old.getAddress());
                    log.info("job [{}] position reporter changed old [{}], now [{}]", position.getJobID(), old, position);
                    try {
                        historyService.save(history);
                    } catch (Exception e) {
                        log.warn("save mysql position reporter changed history fail", e);
                    }
                }
            }
        }
    }
}




