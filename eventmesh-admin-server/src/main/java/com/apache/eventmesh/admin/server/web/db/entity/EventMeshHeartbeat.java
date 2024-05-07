package com.apache.eventmesh.admin.server.web.db.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @TableName event_mesh_heartbeat
 */
@TableName(value ="event_mesh_heartbeat")
@Data
public class EventMeshHeartbeat implements Serializable {
    private Integer id;

    private String adminAddr;

    private String runtimeAddr;

    private Integer jobID;

    private Integer positionID;

    private Date reportTime;

    private Date updateTime;

    private static final long serialVersionUID = 1L;
}