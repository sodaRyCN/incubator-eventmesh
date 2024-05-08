/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.common.remote.offset;

import java.util.Objects;

public class RecordPosition {

    private final RecordPartition recordPartition;

    private final Class<? extends RecordPartition> recordPartitionClazz;

    private final RecordOffset recordOffset;

    private final Class<? extends RecordOffset> recordOffsetClazz;

    public RecordPosition(
        RecordPartition recordPartition, RecordOffset recordOffset) {
        this.recordPartition = recordPartition;
        this.recordOffset = recordOffset;
        this.recordPartitionClazz = recordPartition.getRecordPartitionClass();
        this.recordOffsetClazz = recordOffset.getRecordOffsetClass();
    }

    public RecordPartition getPartition() {
        return recordPartition;
    }

    public RecordOffset getOffset() {
        return recordOffset;
    }

    public Class<? extends RecordPartition> getRecordPartitionClazz() {
        return recordPartitionClazz;
    }

    public Class<? extends RecordOffset> getRecordOffsetClazz() {
        return recordOffsetClazz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RecordPosition)) {
            return false;
        }
        RecordPosition position = (RecordPosition) o;
        return recordPartition.equals(position.recordPartition) && recordOffset.equals(position.recordOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordPartition, recordOffset);
    }
}
