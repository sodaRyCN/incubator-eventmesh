package com.apache.eventmesh.admin.server.web.db.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;

/**
 * @TableName event_mesh_heartbeat
 */
@TableName(value ="event_mesh_heartbeat")
@Data
public class EventMeshHeartbeat implements Serializable {
    @TableId(type = IdType.AUTO)
    private Integer id;

    private String adminAddr;

    private String runtimeAddr;

    private Integer jobID;

    private String reportTime;

    private String updateTime;

    private static final long serialVersionUID = 1L;
}