package de.tum.i13;

import de.tum.i13.challenge.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class EMACalculator {
    public static final int NUM_THREADS = 4;
    private final ExecutorService processPool = Executors.newFixedThreadPool(NUM_THREADS);
    private final ExecutorService emaPool = Executors.newFixedThreadPool(1);
    private final ChallengerGrpc.ChallengerBlockingStub client;
    private final Benchmark benchmark;
    private final BlockingQueue<Window> bq = new LinkedBlockingQueue<>();
    private final Object condition = new Object();
    private boolean done = false;
    private final Object finish = new Object();


    public EMACalculator(ChallengerGrpc.ChallengerBlockingStub client, Benchmark b) {
        this.client = client;
        this.benchmark = b;
    }

    public Batch getBatch() {
        synchronized (condition) {
            if (done) return null;

            Batch b = client.nextBatch(this.benchmark);
            if (b.getLast()) done = true;
            return b;
        }
    }

    public void start() {
        for (int i = 0; i < NUM_THREADS; i++) {
            processPool.submit(
                    new BatchProcessor(this)
            );
        }
        emaPool.submit(new EmaProcessor(this));
    }

    public void awaitTermination() {
        processPool.shutdown();
        emaPool.shutdown();

        try {
            while (!done) {
                processPool.awaitTermination(1, TimeUnit.SECONDS);
                emaPool.awaitTermination(1, TimeUnit.SECONDS);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class Window {
        private final HashMap<String, Event> events = new HashMap<>();
        private final long id;
        private final Set<String> symbols;
        private long last = 0;

        public Window(long id, Collection<String> symbols) {
            this.id = id;
            this.symbols = new HashSet<>();
            this.symbols.addAll(symbols);
        }

        public long getId() { return id; }
        public Set<String> getSymbols() { return symbols; }

        public long getLastSec() {
            return last;
        }

        public void addEvent(Event e) {
            events.put(e.getSymbol(), e);
            last = e.getLastTrade().getSeconds();
        }

        Stream<Event> getEvents() {
            return events.keySet().stream().map(events::get);
        }
    }

    private static class BatchProcessor implements Runnable {
        EMACalculator calculator;
        public BatchProcessor(EMACalculator ins) {
            calculator = ins;
        }

        private void finish() {}
        private void processBatch(Batch b) {
            Window w = null;

            for (Event e : b.getEventsList()) {
                if (w == null) {
                    w = new Window(b.getSeqId(), b.getLookupSymbolsList());
                    w.addEvent(e);
                } else {
                    long seq = e.getLastTrade().getSeconds() / (60 * 5);
                    if (seq < w.getLastSec()) {
                        w.addEvent(e);
                    } else {
                        this.calculator.bq.add(w);
                        w = new Window(b.getSeqId(), b.getLookupSymbolsList());
                        w.addEvent(e);
                    }
                }

            }
        }

        @Override
        public void run() {
            while (true) {
                Batch b = calculator.getBatch();

                if (b == null) {
                    finish();
                    return;
                }

                processBatch(b);
            }
        }
    }

    private static class EmaProcessor implements Runnable {
        private static final Logger log = LogManager.getLogger(EmaProcessor.class);
        EMACalculator calculator;
        HashMap<String, Indicator> prev = new HashMap<>();

        public EmaProcessor(EMACalculator ins) {
            calculator = ins;
        }

        private static float computeEma(float price, float prev, int j) {
            float c = (float) 2 / (1 + j);
            float a = price * c;
            float b = prev * (1 - c);
            return a + b;
        }

        private Indicator getEma(Event e) {
            float prev38 = this.prev.get(e.getSymbol()).getEma38();
            float prev100 = this.prev.get(e.getSymbol()).getEma100();

            float new38 = computeEma(e.getLastTradePrice(), prev38, 38);
            float new100 = computeEma(e.getLastTradePrice(), prev100, 100);


            Indicator i = Indicator.newBuilder()
                    .setEma38(new38)
                    .setEma100(new100)
                    .setSymbol(e.getSymbol())
                    .build();
            this.prev.put(i.getSymbol(), i);
            return i;
        }

        public void processWindow(Window w) {
            List<Indicator> q1 = w.getEvents()
                    .map(this::getEma)
                    .filter(i -> w.getSymbols().contains(i.getSymbol()))
                    .collect(Collectors.toList());

            log.info("Sending " + w.getId());
            this.calculator.client.resultQ1(
                    ResultQ1.newBuilder()
                            .addAllIndicators(q1)
                            .setBatchSeqId(w.getId())
                            .build()
            );
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Window w = calculator.bq.poll(10, TimeUnit.SECONDS);

                    if (w == null) {
                        // try again
                        w = calculator.bq.poll(10, TimeUnit.SECONDS);
                        if (w == null) {
                            return;
                        }
                    }
                    processWindow(w);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }
}
