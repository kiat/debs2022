package edu.ut.debs2022;

import java.util.ArrayList;
import java.util.HashMap;
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

	Map<String, EMAs> emaDict;
	Benchmark benchmark;
	ChallengerBlockingStub challengeClient;

	public Consumer(Benchmark benchmark, ChallengerBlockingStub challengeClient) {
		this.benchmark = benchmark;
		this.challengeClient = challengeClient;
		
		this.emaDict = new HashMap<String, EMAs>(Constants.DICT_INIT_SIZE);
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

//		System.out.println(batch.getSeqId());

		float new_ema38, new_ema100;

		int size = batch.getEventsCount();

		List<Indicator> indicatorsList = new ArrayList<Indicator>(size);

		List<CrossoverEvent> crossoverEventsList = new ArrayList<CrossoverEvent>(size);

		List<Event> events = batch.getEventsList();

		for (Event event : events) {

			float price = event.getLastTradePrice();
			String symbol = event.getSymbol();

			Object m_object = emaDict.get(symbol);
			EMAs emas = (EMAs) m_object;

			if (emas != null) {

				// we have already this symbol in Cache.

				float ema38_pre = emas.getEma38();
				float ema100_pre = emas.getEma100();

				new_ema38 = (price * (2 / (1 + 38))) + ema38_pre * (1 - 2 / (1 + 38));
				new_ema100 = (price * (2 / (1 + 100))) + ema100_pre * (1 - 2 / (1 + 100));

				// put back the updated values
				emas.setEma100(new_ema100);
				emas.setEma38(new_ema38);
				emaDict.put(symbol, emas);

				// Now we check for Query 2 here

				CrossoverEvent crossoverEvent = crossoverEvent(event, ema38_pre, ema100_pre, new_ema38, new_ema100);
				if (crossoverEvent != null) {
					crossoverEventsList.add(crossoverEvent);

				}

			} else {
				// We see this symbol for the first time
				new_ema38 = (price * (2 / (1 + 38)));
				new_ema100 = (price * (2 / (1 + 100)));

				EMAs n_emas = new EMAs(new_ema38, new_ema100);
				emaDict.put(symbol, n_emas);
			}

			// Build the indicator and add them to the ArrayList.
			Indicator indicator = Indicator.newBuilder().setEma100(new_ema100).setEma38(new_ema38).build();

			indicatorsList.add(indicator);

		}

		// create a new tuple result for both queries and return.
		TupleListResult tupleListResult = new TupleListResult(indicatorsList, crossoverEventsList);

		return tupleListResult;
	}

//	
//	if ema_38 <= ema_100 and cur_38 > cur_100:
//        type = ch.CrossoverEvent.SignalType.Buy
//    elif ema_38 >= ema_100 and cur_38 < cur_100:
//        type = ch.CrossoverEvent.SignalType.Sell
//
//    if type is not None and e is not None:
//        return ch.CrossoverEvent(
//                ts=e.last_trade,
//                symbol=e.symbol,
//                security_type=e.security_type,
//                signal_type=type
//            )

	public CrossoverEvent crossoverEvent(Event event, float pastEMA38, float pastEMA100, float currEMA38,
			float currEMA100) {

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
