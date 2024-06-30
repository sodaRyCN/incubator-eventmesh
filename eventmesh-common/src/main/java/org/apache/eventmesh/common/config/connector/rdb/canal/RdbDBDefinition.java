package org.apache.eventmesh.common.config.connector.rdb.canal;

import lombok.Data;

import java.util.Set;

/**
 * @Description: as class name
 */
@Data
public class RdbDBDefinition {
    private String schemaName;
    private Set<RdbTableDefinition> tables;
}
