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

package org.apache.eventmesh.connector.canal.sink.connector;

import javafx.fxml.LoadException;

import org.apache.eventmesh.common.config.connector.Config;

import org.apache.eventmesh.common.config.connector.rdb.canal.CanalSinkConfig;
import org.apache.eventmesh.connector.canal.CanalConnectRecord;
import org.apache.eventmesh.connector.canal.DatabaseConnection;
import org.apache.eventmesh.connector.canal.MysqlSqlTemplate;
import org.apache.eventmesh.connector.canal.sink.DbLoadData;
import org.apache.eventmesh.connector.canal.sink.DbLoadData.TableLoadData;
import org.apache.eventmesh.connector.canal.sink.DbLoadMerger;
import org.apache.eventmesh.openconnect.api.ConnectorCreateService;
import org.apache.eventmesh.openconnect.api.connector.ConnectorContext;
import org.apache.eventmesh.openconnect.api.connector.SinkConnectorContext;
import org.apache.eventmesh.openconnect.api.sink.Sink;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.ConnectRecord;


import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;
import org.springframework.util.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CanalSinkConnector implements Sink, ConnectorCreateService<Sink> {

    private CanalSinkConfig sinkConfig;

    private JdbcTemplate jdbcTemplate;

    private MysqlSqlTemplate mysqlSqlTemplate;

    @Override
    public Class<? extends Config> configClass() {
        return CanalSinkConfig.class;
    }

    @Override
    public void init(Config config) throws Exception {
        // init config for canal source connector
        this.sinkConfig = (CanalSinkConfig) config;
    }

    @Override
    public void init(ConnectorContext connectorContext) throws Exception {
        // init config for canal source connector
        SinkConnectorContext sinkConnectorContext = (SinkConnectorContext) connectorContext;
        this.sinkConfig = (CanalSinkConfig) sinkConnectorContext.getSinkConfig();
        DatabaseConnection.sinkConfig = this.sinkConfig;
        DatabaseConnection.initSinkConnection();
        jdbcTemplate = new JdbcTemplate(DatabaseConnection.sinkDataSource);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void commit(ConnectRecord record) {

    }

    @Override
    public String name() {
        return this.sinkConfig.getSinkConnectorConfig().getConnectorName();
    }

    @Override
    public void stop() {

    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) {
        for (ConnectRecord connectRecord : sinkRecords) {
            List<CanalConnectRecord> canalConnectRecordList = (List<CanalConnectRecord>) connectRecord.getData();
            canalConnectRecordList = filterRecord(canalConnectRecordList, sinkConfig);
            if (isDdlDatas(canalConnectRecordList)) {
                doDdl(canalConnectRecordList);
            } else {
                // 进行一次数据合并，合并相同pk的多次I/U/D操作
                canalConnectRecordList = DbLoadMerger.merge(canalConnectRecordList);
                // 按I/U/D进行归并处理
                DbLoadData loadData = new DbLoadData();
                doBefore(canalConnectRecordList, loadData);
                // 执行load操作
                doLoad(context, loadData);

            }

        }
    }

    @Override
    public Sink create() {
        return new CanalSinkConnector();
    }

    /**
     * 分析整个数据，将datas划分为多个批次. ddl sql前的DML并发执行，然后串行执行ddl后，再并发执行DML
     *
     * @return
     */
    private boolean isDdlDatas(List<CanalConnectRecord> canalConnectRecordList) {
        boolean result = false;
        for (CanalConnectRecord canalConnectRecord : canalConnectRecordList) {
            result |= canalConnectRecord.getEventType().isDdl();
            if (result && !canalConnectRecord.getEventType().isDdl()) {
                throw new RuntimeException("ddl/dml can't be in one batch, it's may be a bug , pls submit issues.");
            }
        }
        return result;
    }

    /**
     * 过滤掉不需要处理的数据
     */
    private List<CanalConnectRecord> filterRecord(List<CanalConnectRecord> canalConnectRecordList, CanalSinkConfig sinkConfig) {
        return canalConnectRecordList.stream()
            .filter(record -> sinkConfig.getSinkConnectorConfig().getSchemaName().equals(record.getSchemaName()) &&
                sinkConfig.getSinkConnectorConfig().getTableName().equals(record.getTableName()))
            .collect(Collectors.toList());
    }

    /**
     * 执行ddl的调用，处理逻辑比较简单: 串行调用
     */
    private void doDdl(List<CanalConnectRecord> canalConnectRecordList) {
        for (final CanalConnectRecord record : canalConnectRecordList) {
            try {
                Boolean result = jdbcTemplate.execute(new StatementCallback<Boolean>() {

                    public Boolean doInStatement(Statement stmt) throws SQLException, DataAccessException {
                        boolean result = true;
                        if (StringUtils.isNotEmpty(record.getDdlSchemaName())) {
                            // 如果mysql，执行ddl时，切换到在源库执行的schema上
                            // result &= stmt.execute("use " +
                            // data.getDdlSchemaName());

                            // 解决当数据库名称为关键字如"Order"的时候,会报错,无法同步
                            result &= stmt.execute("use `" + record.getDdlSchemaName() + "`");
                        }
                        result &= stmt.execute(record.getSql());
                        return result;
                    }
                });

            } catch (Throwable e) {
                throw new RuntimeException(e);
            }

        }
    }

    /**
     * 执行数据处理，比如数据冲突检测
     */
    private void doBefore(List<CanalConnectRecord> canalConnectRecordList, final DbLoadData loadData) {
        for (final CanalConnectRecord record : canalConnectRecordList) {
            boolean filter = interceptor.before(context, record);
            if (!filter) {
                loadData.merge(item);// 进行分类
            }
        }
    }

    private void doLoad(final DbLoadContext context, DbLoadData loadData) {
        // 优先处理delete,可以利用batch优化
        List<List<CanalConnectRecord>> batchDatas = new ArrayList<>();
        for (TableLoadData tableData : loadData.getTables()) {
            if (useBatch) {
                // 优先执行delete语句，针对uniqe更新，一般会进行delete + insert的处理模式，避免并发更新
                batchDatas.addAll(split(tableData.getDeleteDatas()));
            } else {
                // 如果不可以执行batch，则按照单条数据进行并行提交
                // 优先执行delete语句，针对uniqe更新，一般会进行delete + insert的处理模式，避免并发更新
                for (CanalConnectRecord data : tableData.getDeleteDatas()) {
                    batchDatas.add(Arrays.asList(data));
                }
            }
        }

        if (context.getPipeline().getParameters().isDryRun()) {
            doDryRun(context, batchDatas, true);
        } else {
            doTwoPhase(context, batchDatas, true);
        }
        batchDatas.clear();

        // 处理下insert/update
        for (TableLoadData tableData : loadData.getTables()) {
            if (useBatch) {
                // 执行insert + update语句
                batchDatas.addAll(split(tableData.getInsertDatas()));
                batchDatas.addAll(split(tableData.getUpdateDatas()));// 每条记录分为一组，并行加载
            } else {
                // 执行insert + update语句
                for (CanalConnectRecord data : tableData.getInsertDatas()) {
                    batchDatas.add(Arrays.asList(data));
                }
                for (CanalConnectRecord data : tableData.getUpdateDatas()) {
                    batchDatas.add(Arrays.asList(data));
                }
            }
        }

        if (context.getPipeline().getParameters().isDryRun()) {
            doDryRun(context, batchDatas, true);
        } else {
            doTwoPhase(context, batchDatas, true);
        }
        batchDatas.clear();
    }

    /**
     * 将对应的数据按照sql相同进行batch组合
     */
    private List<List<CanalConnectRecord>> split(List<CanalConnectRecord> datas) {
        List<List<CanalConnectRecord>> result = new ArrayList<>();
        if (datas == null || datas.size() == 0) {
            return result;
        } else {
            int[] bits = new int[datas.size()];// 初始化一个标记，用于标明对应的记录是否已分入某个batch
            for (int i = 0; i < bits.length; i++) {
                // 跳过已经被分入batch的
                while (i < bits.length && bits[i] == 1) {
                    i++;
                }

                if (i >= bits.length) { // 已处理完成，退出
                    break;
                }

                // 开始添加batch，最大只加入batchSize个数的对象
                List<CanalConnectRecord> batch = new ArrayList<>();
                bits[i] = 1;
                batch.add(datas.get(i));
                for (int j = i + 1; j < bits.length && batch.size() < batchSize; j++) {
                    if (bits[j] == 0 && canBatch(datas.get(i), datas.get(j))) {
                        batch.add(datas.get(j));
                        bits[j] = 1;// 修改为已加入
                    }
                }
                result.add(batch);
            }

            return result;
        }
    }

    /**
     * 判断两条记录是否可以作为一个batch提交，主要判断sql是否相等. 可优先通过schemaName进行判断
     */
    private boolean canBatch(CanalConnectRecord source, CanalConnectRecord target) {
        return StringUtils.equals(source.getSchemaName(),
            target.getSchemaName())
            && StringUtils.equals(source.getTableName(), target.getTableName())
            && StringUtils.equals(source.getSql(), target.getSql());
//         return StringUtils.equals(source.getSql(), target.getSql());

        // 因为sqlTemplate构造sql时用了String.intern()的操作，保证相同字符串的引用是同一个，所以可以直接使用==进行判断，提升效率
//        return source.getSql() == target.getSql();
    }

    /**
     * 首先进行并行执行，出错后转为串行执行
     */
    private void doTwoPhase(DbLoadContext context, List<List<CanalConnectRecord>> totalRows, boolean canBatch) {
        // 预处理下数据
        List<Future<Exception>> results = new ArrayList<Future<Exception>>();
        for (List<EventData> rows : totalRows) {
            if (CollectionUtils.isEmpty(rows)) {
                continue; // 过滤空记录
            }

            results.add(executor.submit(new DbLoadWorker(context, rows, canBatch)));
        }

        boolean partFailed = false;
        for (int i = 0; i < results.size(); i++) {
            Future<Exception> result = results.get(i);
            Exception ex = null;
            try {
                ex = result.get();
                for (EventData data : totalRows.get(i)) {
                    interceptor.after(context, data);// 通知加载完成
                }
            } catch (Exception e) {
                ex = e;
            }

            if (ex != null) {
                logger.warn("##load phase one failed!", ex);
                partFailed = true;
            }
        }

        if (true == partFailed) {
            // if (CollectionUtils.isEmpty(context.getFailedDatas())) {
            // logger.error("##load phase one failed but failedDatas is empty!");
            // return;
            // }

            // 尝试的内容换成phase one跑的所有数据，避免因failed datas计算错误而导致丢数据
            List<EventData> retryEventDatas = new ArrayList<EventData>();
            for (List<EventData> rows : totalRows) {
                retryEventDatas.addAll(rows);
            }

            context.getFailedDatas().clear(); // 清理failed data数据

            // 可能为null，manager老版本数据序列化传输时，因为数据库中没有skipLoadException变量配置
            Boolean skipLoadException = context.getPipeline().getParameters().getSkipLoadException();
            if (skipLoadException != null && skipLoadException) {// 如果设置为允许跳过单条异常，则一条条执行数据load，准确过滤掉出错的记录，并进行日志记录
                for (EventData retryEventData : retryEventDatas) {
                    DbLoadWorker worker = new DbLoadWorker(context, Arrays.asList(retryEventData), false);// 强制设置batch为false
                    try {
                        Exception ex = worker.call();
                        if (ex != null) {
                            // do skip
                            logger.warn("skip exception for data : {} , caused by {}",
                                retryEventData,
                                ExceptionUtils.getFullStackTrace(ex));
                        }
                    } catch (Exception ex) {
                        // do skip
                        logger.warn("skip exception for data : {} , caused by {}",
                            retryEventData,
                            ExceptionUtils.getFullStackTrace(ex));
                    }
                }
            } else {
                // 直接一批进行处理，减少线程调度
                DbLoadWorker worker = new DbLoadWorker(context, retryEventDatas, false);// 强制设置batch为false
                try {
                    Exception ex = worker.call();
                    if (ex != null) {
                        throw ex; // 自己抛自己接
                    }
                } catch (Exception ex) {
                    logger.error("##load phase two failed!", ex);
                    throw new LoadException(ex);
                }
            }

            // 清理failed data数据
            for (EventData data : retryEventDatas) {
                interceptor.after(context, data);// 通知加载完成
            }
        }

    }
}
