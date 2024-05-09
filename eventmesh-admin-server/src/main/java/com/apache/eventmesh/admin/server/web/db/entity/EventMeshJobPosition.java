package com.apache.eventmesh.admin.server.web.db.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @TableName event_mesh_job_position
 */
@TableName(value ="event_mesh_job_position")
@Data
public class EventMeshJobPosition implements Serializable {
    @TableId(type = IdType.AUTO)
    private Integer id;

    private Integer jobID;

    private String address;

    private Long position;

    private Date createTime;

    private Date updateTime;

    private static final long serialVersionUID = 1L;
}