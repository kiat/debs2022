package de.tum.i13.Cloud;


import com.google.protobuf.Empty;
import de.tum.i13.challenge.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;


public class WorkerService extends WorkerGrpc.WorkerImplBase {
    private static final Logger log = LogManager.getLogger(WorkerService.class);

    private final boolean isMaster;
    private final String host;
    private final int port;

    private final Server server;
    private final EMACalculator emaCalculator = new EMACalculator();
    private ChallengerGrpc.ChallengerBlockingStub challengeClient;

    public WorkerService(String host, int port, boolean isMaster) {
        this.host = host;
        this.port = port;
        this.isMaster = isMaster;

        this.server = ServerBuilder.forPort(port)
                .addService(this)
                .build();

        if (isMaster) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress("challenge.msrg.in.tum.de", 5023)
                    //.forAddress("192.168.1.4", 5023) //in case it is used internally
                    .usePlaintext()
                    .build();
            this.challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                    .withMaxInboundMessageSize(100 * 1024 * 1024)
                    .withMaxOutboundMessageSize(100 * 1024 * 1024);
            this.emaCalculator.initialize(this.challengeClient);
            this.emaCalculator.start(this.challengeClient);
        }
    }

    public void start() {
        try {
            this.server.start();
        } catch (IOException e) {
            log.error("could not start server", e);
        }
    }

    public void awaitTermination() {
        try {
            this.server.awaitTermination();
        } catch (InterruptedException e) {
            log.warn("server interrupted");
        }
    }

    public static WorkerService fromConfig(int hostNum, Configuration conf) {
        Configuration.HostConfig hostConf = conf.getHost(hostNum);
        return new WorkerService(hostConf.getHost(), hostConf.getPort(), hostConf.isMaster());
    }

    @Override
    public void initialize(Benchmark request, StreamObserver<Empty> responseObserver) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                //.forAddress("192.168.1.4", 5023) //in case it is used internally
                .usePlaintext()
                .build();
        this.challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);
        emaCalculator.start(this.challengeClient);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void broadcast(EMABroadcast request, StreamObserver<Empty> responseObserver) {
        this.emaCalculator.addEMAs(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void broadcastWindow(Window request, StreamObserver<Empty> responseObserver) {
        this.emaCalculator.addWindow(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
