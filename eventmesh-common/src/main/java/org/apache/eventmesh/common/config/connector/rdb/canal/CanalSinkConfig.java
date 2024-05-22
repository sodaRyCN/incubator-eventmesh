package org.apache.eventmesh.common.config.connector.rdb.canal;

import org.apache.eventmesh.common.config.connector.SinkConfig;
import org.apache.eventmesh.common.remote.job.SyncMode;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class CanalSinkConfig extends SinkConfig {

    private SyncMode syncMode;                                                 // 同步模式：字段/整条记录

    public SinkConnectorConfig sinkConnectorConfig;

}
