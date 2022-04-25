package de.tum.i13;

import java.util.Date;

import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.BenchmarkConfiguration;
import de.tum.i13.challenge.ChallengerGrpc;
import de.tum.i13.challenge.Query;
import edu.ut.debs2022.Consumer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Main {

	public static void main(String[] args) {

		ManagedChannel channel = ManagedChannelBuilder
				.forAddress("challenge.msrg.in.tum.de", 5023)
				// .forAddress("192.168.1.4", 5023) //in case it is used internally
				.usePlaintext().build();

		var challengeClient = ChallengerGrpc.newBlockingStub(channel) // for demo, we show the blocking stub
				.withMaxInboundMessageSize(100 * 1024 * 1024)
				.withMaxOutboundMessageSize(100 * 1024 * 1024);

		BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
				.setBenchmarkName("Java Test Run on " + new Date().toString())
				.addQueries(Query.Q1)
				.addQueries(Query.Q2)
				.setToken("zqultcyalnowfgxjlzlsztkcquycninr") // go to: https://challenge.msrg.in.tum.de/profile/
				// .setBenchmarkType("Evaluation") //Benchmark Type for evaluation
				.setBenchmarkType("Test") // Benchmark Type for testing
				.build();

		// Create a new Benchmark
		Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);

		// Start the benchmark
		challengeClient.startBenchmark(newBenchmark);

		// Process the events
		int cnt = 0;

		// Create a Consumer

//        Consumer m_consumer= new Consumer(); 

		while (true) {

			Batch batch = challengeClient.nextBatch(newBenchmark);

			if (batch.getLast()) { // Stop when we get the last batch
				System.out.println("Received lastbatch, finished!");
				break;
			}

//            m_consumer.processQuery1(batch, newBenchmark, challengeClient);

//            //process the batch of events we have
//            var q1Results = calculateIndicators(batch);
//
//            ResultQ1 q1Result = ResultQ1.newBuilder()
//                    .setBenchmarkId(newBenchmark.getId()) //set the benchmark id
//                    .setBatchSeqId(batch.getSeqId()) //set the sequence number
//                    .addAllIndicators(q1Results)
//                    .build();
//
//            //return the result of Q1
//            challengeClient.resultQ1(q1Result);
//
//
//            var crossOverevents = calculateCrossoverEvents(batch);
//
//            ResultQ2 q2Result = ResultQ2.newBuilder()
//                    .setBenchmarkId(newBenchmark.getId()) //set the benchmark id
//                    .setBatchSeqId(batch.getSeqId()) //set the sequence number
//                    .addAllCrossoverEvents(crossOverevents)
//                    .build();
//
//            challengeClient.resultQ2(q2Result);
//            

			cnt++;
			System.out.println(cnt);

//            if(cnt > 100) { //for testing you can stop early, in an evaluation run, run until getLast() is True.
//                break;
//            }

		}

		challengeClient.endBenchmark(newBenchmark);
		System.out.println("ended Benchmark");

	}

//    private static List<Indicator> calculateIndicators(Batch batch) {
//        //TODO: improve implementation
//
//        return new ArrayList<>();
//    }
//
//    private static List<CrossoverEvent> calculateCrossoverEvents(Batch batch) {
//        //TODO: improve this implementation
//
//        return new ArrayList<>();
//    }
//    

}
