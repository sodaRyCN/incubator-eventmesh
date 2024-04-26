package org.apache.eventmesh.runtime.connector;

import org.apache.eventmesh.api.consumer.Consumer;
import org.apache.eventmesh.api.factory.StoragePluginFactory;
import org.apache.eventmesh.api.producer.Producer;
import org.apache.eventmesh.common.ThreadPoolFactory;
import org.apache.eventmesh.common.adminserver.HeartBeat;
import org.apache.eventmesh.common.adminserver.request.FetchJobRequest;
import org.apache.eventmesh.common.adminserver.response.FetchJobResponse;
import org.apache.eventmesh.common.config.ConfigService;
import org.apache.eventmesh.common.config.connector.SinkConfig;
import org.apache.eventmesh.common.config.connector.SourceConfig;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc.AdminServiceBlockingStub;
import org.apache.eventmesh.common.protocol.grpc.adminserver.AdminServiceGrpc.AdminServiceStub;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Metadata;
import org.apache.eventmesh.common.protocol.grpc.adminserver.Payload;
import org.apache.eventmesh.common.utils.IPUtils;
import org.apache.eventmesh.common.utils.JsonUtils;
import org.apache.eventmesh.openconnect.api.ConnectorCreateService;
import org.apache.eventmesh.openconnect.api.connector.SinkConnectorContext;
import org.apache.eventmesh.openconnect.api.connector.SourceConnectorContext;
import org.apache.eventmesh.openconnect.api.factory.ConnectorPluginFactory;
import org.apache.eventmesh.openconnect.api.sink.Sink;
import org.apache.eventmesh.openconnect.api.source.Source;
import org.apache.eventmesh.openconnect.offsetmgmt.api.data.ConnectRecord;
import org.apache.eventmesh.openconnect.util.ConfigUtil;
import org.apache.eventmesh.runtime.Runtime;
import org.apache.eventmesh.runtime.RuntimeInstanceConfig;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.google.protobuf.Any;
import com.google.protobuf.UnsafeByteOperations;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConnectorRuntime implements Runtime {

    private RuntimeInstanceConfig runtimeInstanceConfig;

    private ConnectorRuntimeConfig connectorRuntimeConfig;

    private ManagedChannel channel;

    private AdminServiceStub adminServiceStub;

    private AdminServiceBlockingStub adminServiceBlockingStub;

    StreamObserver<Payload> responseObserver;

    StreamObserver<Payload> requestObserver;

    private Source sourceConnector;

    private Sink sinkConnector;

    private Producer producer;

    private Consumer consumer;

    private final ExecutorService sourceService =
        ThreadPoolFactory.createSingleExecutor("eventMesh-sourceService");

    private final ExecutorService startService =
        ThreadPoolFactory.createSingleExecutor("eventMesh-sourceWorker-startService");

    private final ScheduledExecutorService heartBeatExecutor = Executors.newSingleThreadScheduledExecutor();

    private final BlockingQueue<ConnectRecord> queue;

    private volatile boolean isRunning = false;


    public ConnectorRuntime(RuntimeInstanceConfig runtimeInstanceConfig) {
        this.runtimeInstanceConfig = runtimeInstanceConfig;
        this.queue = new LinkedBlockingQueue<>(1000);
    }

    @Override
    public void init() throws Exception {
        // create gRPC channel
        channel = ManagedChannelBuilder.forTarget(runtimeInstanceConfig.getAdminServerAddr())
            .usePlaintext()
            .build();

        adminServiceStub = AdminServiceGrpc.newStub(channel).withWaitForReady();

        adminServiceBlockingStub = AdminServiceGrpc.newBlockingStub(channel).withWaitForReady();

        producer = StoragePluginFactory.getMeshMQProducer(runtimeInstanceConfig.getStoragePluginType());

        consumer = StoragePluginFactory.getMeshMQPushConsumer(runtimeInstanceConfig.getStoragePluginType());

        responseObserver = new StreamObserver<Payload>() {
            @Override
            public void onNext(Payload response) {
                log.info("runtime receive message: {} ", response);
            }

            @Override
            public void onError(Throwable t) {
                log.error("runtime receive error message: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                log.info("runtime finished receive message and completed");
            }
        };

        requestObserver = adminServiceStub.invokeBiStream(responseObserver);

        connectorRuntimeConfig = ConfigService.getInstance().buildConfigInstance(ConnectorRuntimeConfig.class);

        FetchJobResponse jobResponse = fetchJobConfig();

        if (jobResponse == null) {
            throw new RuntimeException("fetch job config fail");
        }

        connectorRuntimeConfig.setSourceConnectorType(jobResponse.getTransportType().getSrc().getName());
        connectorRuntimeConfig.setSourceConnectorDesc(jobResponse.getSourceConnectorDesc());
        connectorRuntimeConfig.setSourceConnectorConfig(jobResponse.getSourceConnectorConfig());

        connectorRuntimeConfig.setSinkConnectorType(jobResponse.getTransportType().getDst().getName());
        connectorRuntimeConfig.setSinkConnectorDesc(jobResponse.getSinkConnectorDesc());
        connectorRuntimeConfig.setSinkConnectorConfig(jobResponse.getSinkConnectorConfig());

        ConnectorCreateService<?> sourceConnectorCreateService = ConnectorPluginFactory.createConnector(
            connectorRuntimeConfig.getSourceConnectorType());
        sourceConnector = (Source)sourceConnectorCreateService.create();

        SourceConfig sourceConfig = (SourceConfig) ConfigUtil.parse(connectorRuntimeConfig.getSourceConnectorConfig(), sourceConnector.configClass());
        SourceConnectorContext sourceConnectorContext = new SourceConnectorContext();
        sourceConnectorContext.setSourceConfig(sourceConfig);
//        sourceConnectorContext.setOffsetStorageReader(offsetStorageReader);

        sourceConnector.init(sourceConnectorContext);

        ConnectorCreateService<?> sinkConnectorCreateService = ConnectorPluginFactory.createConnector(connectorRuntimeConfig.getSinkConnectorType());
        sinkConnector = (Sink)sinkConnectorCreateService.create();

        SinkConfig sinkConfig = (SinkConfig) ConfigUtil.parse(connectorRuntimeConfig.getSinkConnectorConfig(), sinkConnector.configClass());
        SinkConnectorContext sinkConnectorContext = new SinkConnectorContext();
        sinkConnectorContext.setSinkConfig(sinkConfig);
        sinkConnector.init(sinkConnectorContext);
    }

    private FetchJobResponse fetchJobConfig() {
        String jobId = connectorRuntimeConfig.getJobID();
        FetchJobRequest jobRequest = new FetchJobRequest();
        jobRequest.setPrimaryKey(jobId);

        Metadata metadata = Metadata.newBuilder()
            .setType(FetchJobRequest.class.getSimpleName())
            .build();

        Payload request = Payload.newBuilder()
            .setMetadata(metadata)
            .setBody(Any.newBuilder().setValue(UnsafeByteOperations.
                unsafeWrap(Objects.requireNonNull(JsonUtils.toJSONBytes(jobRequest)))).build())
            .build();
        Payload response = adminServiceBlockingStub.invoke(request);
        if (response.getMetadata().getType().equals(FetchJobResponse.class.getSimpleName())) {
            return JsonUtils.parseObject(response.getBody().getValue().toStringUtf8(), FetchJobResponse.class);
        }
        return null;
    }

    @Override
    public void start() throws Exception {

        heartBeatExecutor.scheduleAtFixedRate(() -> {

            HeartBeat heartBeat = new HeartBeat();
            heartBeat.setAddress(IPUtils.getLocalAddress());
            heartBeat.setReportedTimeStamp(String.valueOf(System.currentTimeMillis()));
            // TODO: add more info for heartBeat

            Metadata metadata = Metadata.newBuilder()
                .setType(HeartBeat.class.getSimpleName())
                .build();

            Payload request = Payload.newBuilder()
                .setMetadata(metadata)
                .setBody(Any.newBuilder().setValue(UnsafeByteOperations.
                    unsafeWrap(Objects.requireNonNull(JsonUtils.toJSONBytes(heartBeat)))).build())
                .build();

            requestObserver.onNext(request);
        }, 5, 5, TimeUnit.SECONDS);

//        sourceConnector.start();
//        sourceConnector.poll();
// start offsetMgmtService
//        offsetManagementService.start();
        isRunning = true;
//        pollService.execute(this::startPollAndSend);

        sourceService.execute(
            () -> {
                try {
                    startSourceConnector();
                } catch (Exception e) {
                    log.error("source connector [{}] start fail", sourceConnector.name(), e);
//                    this.stop();
                }
            });


    }

    @Override
    public void stop() throws Exception {
        heartBeatExecutor.shutdown();
        requestObserver.onCompleted();
        if (channel != null && !channel.isShutdown()) {
            channel.shutdown();
        }
    }

    private void startSourceConnector() throws Exception {
        sourceConnector.start();
        while (isRunning) {
            List<ConnectRecord> connectorRecordList = sourceConnector.poll();
            if (connectorRecordList != null && !connectorRecordList.isEmpty()) {
                for (ConnectRecord record : connectorRecordList) {
                    queue.put(record);
                }
            }
        }
    }
}