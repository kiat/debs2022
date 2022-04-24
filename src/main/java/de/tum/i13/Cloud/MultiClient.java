package de.tum.i13.Cloud;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.ChallengerGrpc;
import de.tum.i13.challenge.Event;
import de.tum.i13.challenge.ResultQ1;
import de.tum.i13.challenge.ResultQ2;
import de.tum.i13.challenge.ResultRequest;
import de.tum.i13.challenge.ResultResponse;

public class MultiClient {
	private static final Logger log = LogManager.getLogger(MultiClient.class);

	private final WorkerClient[] clients;
	
	private final ThreadPoolExecutor pool;

	public MultiClient(List<String> targets) {
		List<WorkerClient> validClients = new LinkedList<>();

		for (String target : targets) {
			WorkerClient client = WorkerClient.getClient(target);
			if (client.isAvailable())
				validClients.add(client);
		}

		this.clients = validClients.toArray(WorkerClient[]::new);
		this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Utils.Const.N_THREADS);

		log.info(String.format("connected to %d servers", this.clients.length));
	}

	public void awaitTermination() {
		this.pool.shutdown();
		while (true) {
			try {
				if (this.pool.awaitTermination(5, TimeUnit.MINUTES)) {
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

	public void submitMiniBatch(Batch batch, Benchmark benchmark,
			ChallengerGrpc.ChallengerBlockingStub challengeClient) {
		this.pool.execute(new SendMiniBatchTask(batch, this.clients, benchmark, challengeClient));
	}

	private static class SendMiniBatchTask implements Runnable {
		private static final Logger log = LogManager.getLogger(SendMiniBatchTask.class);

		private final Batch batch;
		private final WorkerClient[] clients;
		private final Benchmark benchmark;
		private final ChallengerGrpc.ChallengerBlockingStub challengeClient;

		public SendMiniBatchTask(Batch batch, WorkerClient[] clients, Benchmark benchmark,
				ChallengerGrpc.ChallengerBlockingStub challengeClient) {
			this.benchmark = benchmark;
			this.batch = batch;
			this.clients = clients;
			this.challengeClient = challengeClient;
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

		private ResultResponse getResults() {
			ResultQ1.Builder q1 = ResultQ1.newBuilder().setBenchmarkId(this.benchmark.getId())
					.setBatchSeqId(this.batch.getSeqId());

			ResultQ2.Builder q2 = ResultQ2.newBuilder().setBenchmarkId(this.benchmark.getId())
					.setBatchSeqId(this.batch.getSeqId());

			ResultRequest request = ResultRequest.newBuilder().setSeqId(this.batch.getSeqId()).build();

			Arrays.stream(this.clients).map(client -> client.getResults(request)).forEach(resultResponse -> {
				synchronized (q1) {
					q1.addAllIndicators(resultResponse.getQuery1Result().getIndicatorsList());
				}
				synchronized (q2) {
					q2.addAllCrossoverEvents(resultResponse.getQuery2Result().getCrossoverEventsList());
				}
			});

			return ResultResponse.newBuilder().setSeqId(this.batch.getSeqId()).setQuery1Result(q1).setQuery2Result(q2)
					.build();
		}

		private void submitResults() {
			ResultResponse results = getResults();

			synchronized (this.challengeClient) {
				this.challengeClient.resultQ1(results.getQuery1Result());
				this.challengeClient.resultQ2(results.getQuery2Result());
			}
		}

		@Override
		public void run() {
			ArrayList<Batch.Builder> batches = new ArrayList<>();

			for (int i = 0; i < this.clients.length; i++) {
				batches.add(Batch.newBuilder().setSeqId(this.batch.getSeqId()));
			}
			for (String sym : this.batch.getLookupSymbolsList()) {
				batches.get(getHash(sym)).addLookupSymbols(sym);
			}

			for (Event e : batch.getEventsList()) {
				batches.get(getHash(e.getSymbol())).addEvents(e);
			}

			sendBatches(batches);
			submitResults();
		}
	}
}
