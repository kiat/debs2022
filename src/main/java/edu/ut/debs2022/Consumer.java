package edu.ut.debs2022;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.ChallengerGrpc.ChallengerBlockingStub;
import de.tum.i13.challenge.Event;
import de.tum.i13.challenge.Indicator;
import de.tum.i13.challenge.ResultQ1;

public class Consumer {

	Map<String, EMAs> emaDict;
	
	

	public Consumer() {
		this.emaDict = new HashMap<String, EMAs>(10000);
	}

	
	
	public void processQuery1(Batch batch, Benchmark newBenchmark, ChallengerBlockingStub challengeClient) {
		var q1Results = calculateIndicators(batch);

		ResultQ1 q1Result = ResultQ1.newBuilder()
				.setBenchmarkId(newBenchmark.getId()) // set the benchmark id
				.setBatchSeqId(batch.getSeqId()) // set the sequence number
				.addAllIndicators(q1Results).build();

		// return the result of Q1
		challengeClient.resultQ1(q1Result);

	}

	
	
	
	private List<Indicator> calculateIndicators(Batch batch) {
		float new_ema38, new_ema100;

		int size = batch.getEventsCount();

		List<Indicator> result = new ArrayList<Indicator>(size);

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

			} else {
				// We see this symbol for the first time
				new_ema38 = (price * (2 / (1 + 38)));
				new_ema100 = (price * (2 / (1 + 100)));

				EMAs n_emas = new EMAs(new_ema38, new_ema100);
				emaDict.put(symbol, n_emas);
			}

			
			// Build the indicator and add them to the ArrayList. 
			Indicator indicator = Indicator.newBuilder()
											.setEma100(new_ema100)
											.setEma38(new_ema38)
											.build();

			result.add(indicator);

		}

		return result;
	}

}
