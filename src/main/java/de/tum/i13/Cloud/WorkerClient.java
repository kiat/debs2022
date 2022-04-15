package de.tum.i13.Cloud;


import com.google.protobuf.Empty;
import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.WorkerGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;


public class WorkerClient {
    private static final Logger log = LogManager.getLogger(WorkerClient.class);
    private final LinkedList<WorkerGrpc.WorkerBlockingStub> workers;

    public WorkerClient(Iterable<Channel> channels) {
        this.workers = new LinkedList<>();
        for (Channel c : channels) {
            this.workers.add(WorkerGrpc.newBlockingStub(c));
        }
    }

    public static WorkerClient fromConfig(int hostNum, Configuration conf) {
        Configuration.HostConfig[] hosts = conf.getHosts();
        LinkedList<Channel> channels = new LinkedList<>();

        for (int i = 0; i < hosts.length; i++) {
            if (i != hostNum) {
                channels.add(ManagedChannelBuilder
                        .forAddress(hosts[i].getHost(), hosts[i].getPort())
                        .usePlaintext()
                        .build()
                );
            }
        }
        return new WorkerClient(channels);
    }
}
