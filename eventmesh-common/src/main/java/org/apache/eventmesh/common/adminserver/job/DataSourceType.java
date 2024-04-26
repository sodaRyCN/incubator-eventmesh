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

    public String getName() {
        return name;
    }

    public DataSourceDriverType getDriverType() {
        return driverType;
    }

    public DataSourceClassify getClassify() {
        return classify;
    }

    public static DataSourceType getDataSourceType(String name) {
        for (DataSourceType dataSourceType : DataSourceType.values()) {
            if (dataSourceType.getName().equals(name)) {
                return dataSourceType;
            }
        }
        return null;
    }
}
