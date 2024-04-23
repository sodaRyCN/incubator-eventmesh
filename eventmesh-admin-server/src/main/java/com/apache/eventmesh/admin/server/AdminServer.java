package com.apache.eventmesh.admin.server;

import org.apache.eventmesh.common.adminserver.Task;
import org.apache.eventmesh.common.Constants;
import org.apache.eventmesh.common.adminserver.HeartBeat;
import org.apache.eventmesh.common.utils.IPUtils;
import org.apache.eventmesh.common.utils.PagedList;
import org.apache.eventmesh.registry.RegisterServerInfo;
import org.apache.eventmesh.registry.RegistryService;

public class AdminServer implements Admin {

    private final RegistryService registryService;

    private final RegisterServerInfo adminServeInfo;

    public AdminServer(RegistryService registryService, AdminServerProperties properties) {
        this.registryService = registryService;
        this.adminServeInfo = new RegisterServerInfo();
        adminServeInfo.setServiceName(Constants.ADMIN_SERVER_REGISTRY_NAME);
        adminServeInfo.setHealth(true);
        adminServeInfo.setAddress(IPUtils.getLocalAddress() + ":" + properties.getPort());
    }


    @Override
    public boolean createOrUpdateTask(Task task) {
        return false;
    }

    @Override
    public boolean deleteTask(Long id) {
        return false;
    }

    @Override
    public Task getTask(Long id) {
        return null;
    }

    @Override
    public PagedList<Task> getTaskPaged(Task task) {
        return null;
    }

    @Override
    public void reportHeartbeat(HeartBeat heartBeat) {

    }

    @Override
    public void start() {
        registryService.register(adminServeInfo);
    }

    @Override
    public void destroy() {
        registryService.unRegister(adminServeInfo);
        registryService.shutdown();
    }
}
