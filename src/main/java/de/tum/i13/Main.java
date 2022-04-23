package de.tum.i13;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.Cloud.MultiClient;
import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.BenchmarkConfiguration;
import de.tum.i13.challenge.ChallengerGrpc;
import de.tum.i13.challenge.CrossoverEvent;
import de.tum.i13.challenge.Indicator;
import de.tum.i13.challenge.Query;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Main {
    private static final Logger log = LogManager.getLogger(Main.class);
    private static int maxBatches = 100;

    public static void main(String[] args) throws MalformedURLException {
        MultiClient workerClient = MultiClient.fromConfiguration("cloud.properties");

        if (args.length > 0) {
            maxBatches = Integer.parseInt(args[0]);
        }

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023)
                //.forAddress("192.168.1.4", 5023) //in case it is used internally
                .usePlaintext()
                .build();


        var challengeClient = ChallengerGrpc.newBlockingStub(channel) //for demo, we show the blocking stub
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
                .setBenchmarkName("Java- " + new Date())
                .addQueries(Query.Q1)
                .addQueries(Query.Q2)
                .setToken("zqultcyalnowfgxjlzlsztkcquycninr") //go to: https://challenge.msrg.in.tum.de/profile/
                //.setBenchmarkType("evaluation") //Benchmark Type for evaluation
                .setBenchmarkType("test") //Benchmark Type for testing
                .build();

        //Create a new Benchmark
        Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);

        //Start the benchmark
        challengeClient.startBenchmark(newBenchmark);

        //Process the events
        while(true) {
            Batch batch;

            synchronized (challengeClient) {
                batch = challengeClient.nextBatch(newBenchmark);
            }

//            workerClient.submitMiniBatch(batch, newBenchmark, challengeClient);


            log.info(String.format("processed batch %d with %d events", batch.getSeqId(), batch.getEventsList().size()));

            if (batch.getLast() || batch.getSeqId() >= maxBatches) { //Stop when we get the last batch
                log.info("received last batch");
                break;
            }
        }

        workerClient.awaitTermination();
        
        challengeClient.endBenchmark(newBenchmark);
        
        log.info("finished benchmark");
    }

    private static List<Indicator> calculateIndicators(Batch batch) {
        //TODO: improve implementation

        return new ArrayList<>();
    }

    private static List<CrossoverEvent> calculateCrossoverEvents(Batch batch) {
        //TODO: improve this implementation

        return new ArrayList<>();
    }
}
