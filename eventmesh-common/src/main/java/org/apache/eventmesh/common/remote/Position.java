package org.apache.eventmesh.common.remote;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class Position<T extends Map> {

    List<T> recordPositionList;

}
