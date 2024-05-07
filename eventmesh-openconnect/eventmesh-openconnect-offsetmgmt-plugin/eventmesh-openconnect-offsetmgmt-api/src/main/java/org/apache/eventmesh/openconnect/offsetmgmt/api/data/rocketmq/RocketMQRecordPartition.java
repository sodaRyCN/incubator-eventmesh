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

package org.apache.eventmesh.openconnect.offsetmgmt.api.data.rocketmq;

import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.RecordPartition;
import org.apache.eventmesh.openconnect.offsetmgmt.api.storage.ConnectorRecordPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class RocketMQRecordPartition extends ConnectorRecordPartition {

    /**
     *  key=topic,value=topicName
     *  key=brokerName,value=brokerName
     *  key=queueId,value=queueId
     */


    public RocketMQRecordPartition() {

    }

    public RocketMQRecordPartition(Map<String, ?> partition) {
        super(partition);
    }

    public Class<? extends RecordPartition> getRecordPartitionClass() {
        return RocketMQRecordPartition.class;
    }

}
