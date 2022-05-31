package edu.ut.debs2022;

import java.util.Date;

import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.BenchmarkConfiguration;
import de.tum.i13.challenge.ChallengerGrpc;
import de.tum.i13.challenge.Query;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Main {

	public static void main(String[] args) {

		ManagedChannel channel = ManagedChannelBuilder.forAddress("challenge.msrg.in.tum.de", 5023)
				// .forAddress("192.168.1.4", 5023) //in case it is used internally
				.usePlaintext().build();

//		var challengeClient = ChallengerGrpc.newBlockingStub(channel) // for demo, we show the blocking stub
//				.withMaxInboundMessageSize(100 * 1024 * 1024)
//				.withMaxOutboundMessageSize(100 * 1024 * 1024);

		var challengeClient = ChallengerGrpc.newBlockingStub(channel); // for demo, we show the blocking stub

		BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
				.setBenchmarkName("Java Test Run on " + new Date().toString()).addQueries(Query.Q1).addQueries(Query.Q2)
				.setToken("zqultcyalnowfgxjlzlsztkcquycninr") // go to: https://challenge.msrg.in.tum.de/profile/
				.setBenchmarkType("Evaluation") // Benchmark Type for evaluation
//				.setBenchmarkType("Test") // Benchmark Type for testing
				.build();

//		 Create a new Benchmark
		Benchmark newBenchmark = challengeClient.createNewBenchmark(bc);

		// Start the benchmark
		challengeClient.startBenchmark(newBenchmark);

		Reader my_reader = new Reader(newBenchmark, challengeClient);

		my_reader.start();

		Consumer my_consumer = new Consumer(newBenchmark, challengeClient);

		my_consumer.start();

	}

}
