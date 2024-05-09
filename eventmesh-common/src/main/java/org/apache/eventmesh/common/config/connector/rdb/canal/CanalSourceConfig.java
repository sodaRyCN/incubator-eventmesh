package org.apache.eventmesh.common.config.connector.rdb.canal;

import org.apache.eventmesh.common.config.connector.SourceConfig;
import org.apache.eventmesh.common.config.connector.rdb.jdbc.SourceConnectorConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class CanalSourceConfig extends SourceConfig {

    private SourceConnectorConfig sourceConnectorConfig;
}
