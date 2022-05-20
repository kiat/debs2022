package edu.ut.debs2022;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.ChallengerGrpc.ChallengerBlockingStub;
import de.tum.i13.challenge.CrossoverEvent;
import de.tum.i13.challenge.Event;
import de.tum.i13.challenge.Indicator;
import de.tum.i13.challenge.ResultQ1;
import de.tum.i13.challenge.ResultQ2;

public class Consumer extends Thread {

	Map<String, Tracker> trackerDict;
	Benchmark benchmark;
	ChallengerBlockingStub challengeClient;

	public Consumer(Benchmark benchmark, ChallengerBlockingStub challengeClient) {
		this.benchmark = benchmark;
		this.challengeClient = challengeClient;
		
		this.trackerDict = new HashMap<String, Tracker>(Constants.DICT_INIT_SIZE);
	}

	public void run() {

		Batch batch;

		while (true) {

			if (BatchCacheSingleton.getInstance().size() == 0) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			} else {

				batch = BatchCacheSingleton.getInstance().getNext();
				TupleListResult results = calculateIndicators(batch);

				ResultQ1 q1Result = ResultQ1.newBuilder()
						.setBenchmarkId(this.benchmark.getId()) // set the benchmark id
						.setBatchSeqId(batch.getSeqId()) // set the sequence number
						.addAllIndicators(results.getIndicatorList())
						.build();

				ResultQ2 q2Result = ResultQ2.newBuilder()
						.setBenchmarkId(this.benchmark.getId()) // set the benchmark id
						.setBatchSeqId(batch.getSeqId()) // set the sequence number
						.addAllCrossoverEvents(results
						.getCrossoverEventList())
						.build();

				// return the result of Q1
				challengeClient.resultQ1(q1Result);

				challengeClient.resultQ2(q2Result);

				if (batch.getLast()) { // Stop when we get the last batch
					System.out.println("Received lastbatch on the Consumer Side, finished!");
					break;
				}

			}
		}

		challengeClient.endBenchmark(this.benchmark);
		System.out.println("ended Benchmark");

	}

	/**
	 * 
	 * @param batch
	 * @return
	 */
	private TupleListResult calculateIndicators(Batch batch) {

		int size = batch.getEventsCount();

		List<Indicator> indicatorsList = new ArrayList<Indicator>(size);

		List<CrossoverEvent> crossoverEventsList = new ArrayList<CrossoverEvent>(size);

		List<Event> events = batch.getEventsList();

		long startTime = -1;

		for (int i = 0; i < events.size(); i++) {

			Event e = events.get(i);
			
			// First event, set start time.
			if (i == 0) {
				startTime = e.getLastTrade().getSeconds();
			}

			if (!trackerDict.containsKey(e.getSymbol())) {
				Tracker t = new Tracker(e.getSymbol(), startTime);
			}

			Tracker t = trackerDict.get(e.getSymbol());

			t.evalEvent(e);

		}

		List<String> lookup = batch.getLookupSymbolsList();

		// Only get indicators/crossovers for look up symbols.
		for (String symbol: lookup) {

			if (!trackerDict.containsKey(symbol)) {
				continue;
			}

			Tracker t = trackerDict.get(symbol);

			float ema38 = t.getEma38();
			float ema100 = t.getEma100();

			Indicator indicator = Indicator.newBuilder()
					.setSymbol(symbol)
					.setEma38(ema38)
					.setEma100(ema100)
					.build();

			indicatorsList.add(indicator);
			crossoverEventsList.addAll(t.getCrossoverEvents());

		}

		// create a new tuple result for both queries and return.
		TupleListResult tupleListResult = new TupleListResult(indicatorsList, crossoverEventsList);

		return tupleListResult;
	}


	private class Tracker {

		private String symbol;
		private long startTime;
		private float prevEma38;
		private float prevEma100;
		private Event latestEvent;
		private List<CrossoverEvent> crossoverEvents;


		public Tracker(String symbol, long reference) {
			this.symbol = symbol;
			this.startTime = reference;
			this.prevEma38 = 0;
			this.prevEma100 = 0;
			this.latestEvent = null;
			this.crossoverEvents = new LinkedList<CrossoverEvent>();
		}

		public void evalEvent(Event event) {

			long index = (event.getLastTrade().getSeconds() - this.startTime) / (5 * 60);
			
			// Found a later event for the current window,
			// update latest event.
			if (index == 0) {
				this.latestEvent = event;
				return;
			}

			float price = latestEvent == null ? 0 : latestEvent.getLastTradePrice();

			float ema38 = (price * (2 / (1 + 38))) + this.prevEma38 * (1 - 2 / (1 + 38));
			float ema100 = (price * (2 / (1 + 100))) + this.prevEma100 * (1 - 2 / (1 + 100));
		

			CrossoverEvent crossoverEvent = this.detectCrossoverEvent(
				event,
				this.prevEma38,
				this.prevEma100,
				ema38,
				ema100
			);

			if (crossoverEvent != null) {
				this.crossoverEvents.add(crossoverEvent);

				while (this.crossoverEvents.size() > 3) {
					this.crossoverEvents.remove(0);
				}
			}

			this.prevEma100 = ema100;
			this.prevEma38 = ema38;

			this.startTime += index * (5 * 60);
			this.latestEvent = event;
		}

		public List<CrossoverEvent> getCrossoverEvents() {
			return this.crossoverEvents;
		}

		public float getEma38() {
			return this.prevEma38;
		}

		public float getEma100() {
			return this.prevEma100;
		}

		public CrossoverEvent detectCrossoverEvent (Event event, float pastEMA38, float pastEMA100, float currEMA38, float currEMA100) {

			CrossoverEvent crossoverEvent = null;

			if (pastEMA38 <= pastEMA100 && currEMA38 > currEMA100) {
				crossoverEvent = CrossoverEvent.newBuilder().setSymbol(event.getSymbol())
						.setSignalType(CrossoverEvent.SignalType.Buy).build();

			} else if (pastEMA38 >= pastEMA100 && currEMA38 < currEMA100) {
				crossoverEvent = CrossoverEvent.newBuilder().setSymbol(event.getSymbol())
						.setSignalType(CrossoverEvent.SignalType.Sell).build();
			}

			return crossoverEvent;

		}

	}

}
