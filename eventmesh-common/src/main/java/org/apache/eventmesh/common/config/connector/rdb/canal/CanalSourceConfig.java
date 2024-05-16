package org.apache.eventmesh.common.config.connector.rdb.canal;

import org.apache.eventmesh.common.config.connector.SourceConfig;
import org.apache.eventmesh.common.remote.offset.RecordPosition;

import java.util.List;

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

    private Integer batchSize = 10000;

    private Long batchTimeout = -1L;

    private List<RecordPosition> recordPositions;

    private SourceConnectorConfig sourceConnectorConfig;
}
