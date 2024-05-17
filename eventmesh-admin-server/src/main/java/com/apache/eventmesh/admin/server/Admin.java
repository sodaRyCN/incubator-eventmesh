package com.apache.eventmesh.admin.server;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.eventmesh.common.remote.Task;
import org.apache.eventmesh.common.remote.offset.RecordPosition;
import org.apache.eventmesh.common.remote.request.ReportHeartBeatRequest;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.common.utils.PagedList;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.canal.CanalRecordOffset;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.canal.CanalRecordPartition;

public interface Admin extends ComponentLifeCycle {
    /**
     * support for web or ops
     **/
    boolean createOrUpdateTask(Task task);
    boolean deleteTask(Long id);
    Task getTask(Long id);
    // paged list
    PagedList<Task> getTaskPaged(Task task);

    /**
     * support for task
     */
    void reportHeartbeat(ReportHeartBeatRequest heartBeat);


    static void main(String[] args) {
//        ReportPositionRequest request = new ReportPositionRequest();
//        request.setJobID("1");
//        request.setAddress("1");
//        request.setState(JobState.RUNNING);
//        request.setDataSourceType(DataSourceType.MYSQL);
        RecordPosition recordPosition = new RecordPosition();
        CanalRecordOffset recordOffset = new CanalRecordOffset();
        recordOffset.setOffset(12345L);
        recordPosition.setRecordOffset(recordOffset);
        CanalRecordPartition partition = new CanalRecordPartition();
        partition.setJournalName("demo-binary-log-01");
        partition.setTimeStamp(System.currentTimeMillis());
        recordPosition.setRecordPartition(partition);
//        ArrayList<RecordPosition> list = new ArrayList<>();
//        list.add(recordPosition);
//        request.setRecordPositionList(list);
        String bytes = JsonUtils.toJSONString(recordPosition);

        RecordPosition object1 = JsonUtils.parseTypeReferenceObject(bytes, new TypeReference<RecordPosition>() {});
        RecordPosition object2 = JsonUtils.parseObject(bytes, RecordPosition.class);
        System.out.println(object1);
        System.out.println(object2);
    }

}
