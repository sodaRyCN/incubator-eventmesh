package com.apache.eventmesh.admin.server.web.db.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;

/**
 * @TableName event_mesh_data_source
 */
@TableName(value ="event_mesh_data_source")
@Data
public class EventMeshDataSource implements Serializable {
    private Integer id;

    private Integer type;

    private String address;

    private String desc;

    private String configuration;

    private static final long serialVersionUID = 1L;
}