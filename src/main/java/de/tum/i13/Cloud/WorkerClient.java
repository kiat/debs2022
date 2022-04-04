package de.tum.i13.Cloud;


import com.google.protobuf.Empty;
import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.ResultRequest;
import de.tum.i13.challenge.ResultResponse;
import de.tum.i13.challenge.WorkerGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;


public class WorkerClient {
    private static final Logger log = LogManager.getLogger(WorkerClient.class);
    private final WorkerGrpc.WorkerBlockingStub blockingStub;

    public WorkerClient(Channel channel) {
        this.blockingStub = WorkerGrpc.newBlockingStub(channel);
    }

    public boolean isAvailable() {
        try {
            this.blockingStub.check(Empty.newBuilder().build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void submitMiniBatch(Batch batch) {
        this.blockingStub.submitMiniBatch(batch);
    }

    public ResultResponse getResults(ResultRequest request) {
        return this.blockingStub.getResults(request);
    }

    public static WorkerClient getClient(String target) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        return new WorkerClient(channel);
    }
}
