package org.apache.eventmesh.common.config.connector.rdb.canal;

import org.apache.eventmesh.common.config.connector.SinkConfig;
import org.apache.eventmesh.common.config.connector.rdb.jdbc.SinkConnectorConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class CanalSinkConfig extends SinkConfig {

    public SinkConnectorConfig sinkConnectorConfig;

}
