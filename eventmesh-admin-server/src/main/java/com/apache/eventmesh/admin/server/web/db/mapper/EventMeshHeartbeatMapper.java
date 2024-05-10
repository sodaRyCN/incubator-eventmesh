package com.apache.eventmesh.admin.server.web.db.mapper;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshHeartbeat;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

/**
* @author sodafang
* @description 针对表【event_mesh_heartbeat】的数据库操作Mapper
* @createDate 2024-05-10 16:05:00
* @Entity com.apache.eventmesh.admin.server.web.db.entity.EventMeshHeartbeat
*/
@Mapper
public interface EventMeshHeartbeatMapper extends BaseMapper<EventMeshHeartbeat> {

}




