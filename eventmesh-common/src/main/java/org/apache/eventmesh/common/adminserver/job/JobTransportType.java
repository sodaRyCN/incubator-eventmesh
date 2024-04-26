package org.apache.eventmesh.common.adminserver.job;

public enum JobTransportType {
    MYSQL_MYSQL(DataSourceType.MYSQL, DataSourceType.MYSQL),
    REDIS_REDIS(DataSourceType.REDIS, DataSourceType.REDIS),
    ROCKETMQ_ROCKETMQ(DataSourceType.ROCKETMQ,DataSourceType.ROCKETMQ);

    DataSourceType src;

    DataSourceType dst;

    JobTransportType(DataSourceType src, DataSourceType dst) {
        this.src = src;
        this.dst = dst;
    }

    public DataSourceType getSrc() {
        return src;
    }

    public DataSourceType getDst() {
        return dst;
    }

    public static JobTransportType getJobTransportType(DataSourceType src, DataSourceType dst) {
        for (JobTransportType jobTransportType : JobTransportType.values()) {
            if (jobTransportType.getSrc().equals(src) && jobTransportType.getDst().equals(dst)) {
                return jobTransportType;
            }
        }
        return null;
    }
}
