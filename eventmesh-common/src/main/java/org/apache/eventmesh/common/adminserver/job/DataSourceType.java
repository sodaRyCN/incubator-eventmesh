package org.apache.eventmesh.common.adminserver.job;

public enum DataSourceType {
    MYSQL("MySQL", DataSourceDriverType.MYSQL, DataSourceClassify.RDB),
    REDIS("Redis", DataSourceDriverType.REDIS, DataSourceClassify.CACHE),
    ROCKETMQ("RocketMQ", DataSourceDriverType.ROCKETMQ, DataSourceClassify.MQ);
    private String name;
    private DataSourceDriverType driverType;
    private DataSourceClassify classify;

    DataSourceType(String name, DataSourceDriverType driverType, DataSourceClassify classify) {
        this.name = name;
        this.driverType = driverType;
        this.classify = classify;
    }
}
