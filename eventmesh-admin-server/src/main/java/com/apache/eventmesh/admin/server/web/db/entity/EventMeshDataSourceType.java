package com.apache.eventmesh.admin.server.web.db.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.io.Serializable;
import java.util.Date;
import lombok.Data;

/**
 * @TableName event_mesh_data_source_type
 */
@TableName(value ="event_mesh_data_source_type")
@Data
public class EventMeshDataSourceType implements Serializable {
    private Integer id;

    private String name;

    private String desc;

    private Integer createUid;

    private Integer updateUid;

    private Date createTime;

    private Date updateTime;

    private static final long serialVersionUID = 1L;
}