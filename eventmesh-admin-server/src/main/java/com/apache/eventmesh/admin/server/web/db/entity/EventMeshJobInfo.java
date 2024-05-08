package com.apache.eventmesh.admin.server.web.db.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @TableName event_mesh_job_info
 */
@TableName(value ="event_mesh_job_info")
@Data
public class EventMeshJobInfo implements Serializable {
    private Integer jobID;

    private Integer transportType;

    private Integer sourceData;

    private Integer targetData;

    private Integer state;

    private Integer type;

    private Integer position;

    private Integer createUid;

    private Integer updateUid;

    private Date createTime;

    private Date updateTime;

    private static final long serialVersionUID = 1L;
}