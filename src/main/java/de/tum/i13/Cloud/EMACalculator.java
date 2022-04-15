package de.tum.i13.Cloud;

import com.google.protobuf.Timestamp;
import de.tum.i13.challenge.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EMACalculator {
    private static final Logger log = LogManager.getLogger(EMACalculator.class);
    private final HashMap<Long, EMACache> emaValues = new HashMap<>();


    public void addEMAs(EMABroadcast emas) {

    }

    public void addWindow(Window window) {

    }

    public void start(ChallengerGrpc.ChallengerBlockingStub challengeClient) {

    }

    public void initialize(ChallengerGrpc.ChallengerBlockingStub challengeClient) {
        BenchmarkConfiguration bc = BenchmarkConfiguration.newBuilder()
                .setBenchmarkName("Java- " + new Date())
                .addQueries(Query.Q1)
                .addQueries(Query.Q2)
                .setToken("zqultcyalnowfgxjlzlsztkcquycninr") //go to: https://challenge.msrg.in.tum.de/profile/
                //.setBenchmarkType("evaluation") //Benchmark Type for evaluation
                .setBenchmarkType("test") //Benchmark Type for testing
                .build();
    }


    private static class EMACache {
        private final HashMap<String, Indicator> indicators = new HashMap<>();

        public static EMACache fromIndicators(Iterable<Indicator> indicators) {
            EMACache cache = new EMACache();

            for (Indicator i : indicators) {
                cache.indicators.put(i.getSymbol(), i);
            }

            return cache;
        }

        public EMACache() {}

        public Indicator getIndicator(String symbol) {
            return indicators.get(symbol);
        }

        public boolean containsSymbol(String symbol) {
            return this.indicators.containsKey(symbol);
        }

        public void putSymbol(String symbol, Indicator indicator) {
            this.indicators.put(symbol, indicator);
        }

        public Stream<Indicator> getIndicators(Iterable<String> symbols) {
            return StreamSupport.stream(symbols.spliterator(), false).map(indicators::get);
        }
    }
}
