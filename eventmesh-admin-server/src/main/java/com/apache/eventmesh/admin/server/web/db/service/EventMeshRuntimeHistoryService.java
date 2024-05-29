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

package com.apache.eventmesh.admin.server.web.db.service;

import com.apache.eventmesh.admin.server.web.db.entity.EventMeshRuntimeHistory;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * @author sodafang
 * @description 针对表【event_mesh_runtime_history(记录runtime上运行任务的变更)】的数据库操作Service
 * @createDate 2024-05-14 17:15:03
 */
public interface EventMeshRuntimeHistoryService extends IService<EventMeshRuntimeHistory> {

}
