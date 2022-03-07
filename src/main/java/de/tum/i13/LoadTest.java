package de.tum.i13;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


import org.apache.commons.lang3.time.StopWatch;

import de.tum.i13.challenge.BenchmarkConfiguration;
import de.tum.i13.challenge.ChallengerGrpc;
import de.tum.i13.challenge.Query;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class LoadTest {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LoadTest lt = new LoadTest();
        lt.run();
        return;
    }

    public void run() throws ExecutionException, InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("challenge.msrg.in.tum.de", 5023) //from external
                //.forAddress("192.168.1.4", 5023)// internal API call
                .usePlaintext()
                .maxRetryAttempts(1000)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(15, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(false)
                .enableRetry()
                .build();

        var challengeClient = ChallengerGrpc.newFutureStub(channel)
                .withMaxInboundMessageSize(100 * 1024 * 1024)
                .withMaxOutboundMessageSize(100 * 1024 * 1024);

        BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
                .setBenchmarkName("Testrun " + new Date().toString())
                .addQueries(Query.Q1)
                .addQueries(Query.Q2)
                //.setToken(System.getenv().get("API_TOKEN")) //go to: https://challenge.msrg.in.tum.de/profile/
                .setToken("vcgeajpqzwrfuwytvqyxypjuksgbraeg")
                .setBenchmarkType("test") //Benchmark Type for testing
                //.setBenchmarkType("evaluation") //Benchmark Type for testing
                .build();


        System.out.println("createNewBenchmark");
        var newBenchmark = challengeClient.createNewBenchmark(bc).get();
        challengeClient.startBenchmark(newBenchmark);

        int forTestingBatchCount = 100;

        CountDownLatch latch = new CountDownLatch(forTestingBatchCount);
        ExecutorService executorService = Executors.newFixedThreadPool(16);

        AtomicLong al = new AtomicLong();
        AtomicLong messageCount = new AtomicLong();
        System.out.println("begin");
        StopWatch sw = new StopWatch();
        sw.reset();
        sw.start();
        for(int i = 0; i < forTestingBatchCount; ++i) {
            executorService.submit(() -> {
                try {
                    var batch = challengeClient.nextBatch(newBenchmark).get();
                    al.addAndGet(batch.getSeqId());
                    messageCount.addAndGet(batch.getEventsCount());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                latch.countDown();
            });
        }

        long expectedSumOfSeqIds = 0;
        for(int i = 0; i < forTestingBatchCount; ++i) {
            expectedSumOfSeqIds += i;
        }
        latch.await();
        sw.stop();
        executorService.shutdown();



        System.out.println("Received all batches: " + (expectedSumOfSeqIds == al.get()));
        System.out.println("Amount of batches: " + forTestingBatchCount);

        System.out.format("batches per second throughput: %.2f\n", (double)forTestingBatchCount/(double)sw.getNanoTime()*1_000_000_000.0);
        System.out.format("events per second throughput: %.2f\n", (double)messageCount.get()/(double)sw.getNanoTime()*1_000_000_000.0);
        System.out.println("finished: " + sw.formatTime());
        System.out.println("cnt: " + expectedSumOfSeqIds + " al: " + al.get());

        return;
    }
}