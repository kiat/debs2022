package de.tum.i13.Cloud;


import com.google.protobuf.Empty;
import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.ResultRequest;
import de.tum.i13.challenge.ResultResponse;
import de.tum.i13.challenge.WorkerGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



public class WorkerService extends WorkerGrpc.WorkerImplBase {
    private static final Logger log = LogManager.getLogger(WorkerService.class);

    @Override
    public void submitMiniBatch(Batch request, StreamObserver<Empty> responseObserver) {
        log.info(String.format("received batch %d with %d events", request.getSeqId(), request.getEventsList().size()));
        // TODO: compute EMAs for this mini-batch. Use different thread for this task.

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void getResults(ResultRequest request, StreamObserver<ResultResponse> responseObserver) {
        log.info("requested results for " + request.getSeqId());
        // TODO: return EMAs and crossover indicators

        responseObserver.onNext(ResultResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void check(Empty request, StreamObserver<Empty> responseObserver) {
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
