package org.apache.eventmesh.common.adminserver.job;

public enum JobTransportType {
    MYSQL_MYSQL(DataSourceType.MYSQL, DataSourceType.MYSQL),
    REDIS_REDIS(DataSourceType.REDIS, DataSourceType.REDIS),
    ROCKETMQ_ROCKETMQ(DataSourceType.ROCKETMQ,DataSourceType.ROCKETMQ);


    JobTransportType(DataSourceType src, DataSourceType dst) {

    }
}
