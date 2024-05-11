package org.apache.eventmesh.common.config.connector.rdb.canal;

import org.apache.eventmesh.common.config.connector.SourceConfig;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class CanalSourceConfig extends SourceConfig {

    private String destination;

    private Long canalInstanceId;

    private String desc;

    private boolean ddlSync = true;

    private boolean filterTableError = false;

    private Long slaveId;

    private Short clientId;

    private Integer batchSize;

    private Long batchTimeout;

    private SourceConnectorConfig sourceConnectorConfig;
}
