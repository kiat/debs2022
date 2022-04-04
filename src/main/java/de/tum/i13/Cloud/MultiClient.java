package de.tum.i13.Cloud;

import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Event;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class MultiClient {
    private static final Logger log = LogManager.getLogger(MultiClient.class);

    WorkerClient[] clients;
    ThreadPoolExecutor pool;

    public MultiClient(List<String> targets) {
        List<WorkerClient> validClients = new LinkedList<>();

        for (String target : targets) {
            WorkerClient client = WorkerClient.getClient(target);
            if(client.isAvailable()) validClients.add(client);
        }

        this.clients = validClients.toArray(WorkerClient[]::new);
        this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Utils.Const.N_THREADS);

        log.info(String.format("connected to %d servers", this.clients.length));
    }

    public void awaitTermination() {
        while (true) {
            try {
                if(this.pool.awaitTermination(5, TimeUnit.MINUTES)) {
                    break;
                }
            } catch (InterruptedException e) {
                log.warn("interrupted while awaiting termination");
            }
        }

    }

    public static MultiClient fromConfiguration(Configuration conf) {
        return new MultiClient(conf.getHosts());
    }

    public static MultiClient fromConfiguration(String configFile) {
        return fromConfiguration(new Configuration(configFile));
    }

    public void submitMiniBatch(Batch batch) {
        this.pool.execute(new SendMiniBatchTask(batch, this.clients));
    }

    private static class SendMiniBatchTask implements Runnable {
        private static final Logger log = LogManager.getLogger(SendMiniBatchTask.class);

        private final Batch batch;
        private final WorkerClient[] clients;

        public SendMiniBatchTask(Batch batch, WorkerClient[] clients) {
            this.batch = batch;
            this.clients = clients;
        }

        private int getHash(String key) {
            return Utils.hashIt(key, this.clients.length);
        }

        private void sendBatches(ArrayList<Batch.Builder> batches) {
            for (int i = 0; i < batches.size(); i++) {
                synchronized (this.clients[i]) {
                    this.clients[i].submitMiniBatch(batches.get(i).build());
                }
            }
        }


        @Override
        public void run() {
            ArrayList<Batch.Builder> batches = new ArrayList<>();

            for (int i = 0; i < this.clients.length; i++) {
                batches.add(
                        Batch.newBuilder().setSeqId(this.batch.getSeqId())
                );
            }
            for (String sym : this.batch.getLookupSymbolsList()) {
                batches.get(getHash(sym)).addLookupSymbols(sym);
            }

            for (Event e: batch.getEventsList()) {
                batches.get(getHash(e.getSymbol())).addEvents(e);
            }

            sendBatches(batches);
        }
    }
}
