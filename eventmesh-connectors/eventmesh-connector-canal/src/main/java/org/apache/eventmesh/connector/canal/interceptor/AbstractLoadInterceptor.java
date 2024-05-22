/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.connector.canal.interceptor;

import org.apache.eventmesh.connector.canal.dialect.DbDialect;

import java.util.List;



/**
 * 提供接口的默认实现
 *
 */
public class AbstractLoadInterceptor<L, D> implements LoadInterceptor<L, D> {

    public void prepare(L context) {
    }

    public boolean before(L context, D currentData) {
        return false;
    }

    public void transactionBegin(L context, List<D> currentDatas, DbDialect dialect) {
    }

    public void transactionEnd(L context, List<D> currentDatas, DbDialect dialect) {
    }

    public void after(L context, D currentData) {

    }

    public void commit(L context) {
    }

    public void error(L context) {
    }

}
