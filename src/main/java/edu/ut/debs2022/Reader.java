package edu.ut.debs2022;

import de.tum.i13.challenge.Batch;
import de.tum.i13.challenge.Benchmark;
import de.tum.i13.challenge.ChallengerGrpc.ChallengerBlockingStub;

public class Reader extends Thread {
	// This thread reads the batches from the API and add them to the Queue Cache. 

	Benchmark benchmark;
	ChallengerBlockingStub challengeClient;

	public Reader(Benchmark benchmark, ChallengerBlockingStub challengeClient) {
		this.benchmark = benchmark;
		this.challengeClient = challengeClient;

	}

	public void run() {

		long cnt = 0;

		while (true) {

			Batch batch = this.challengeClient.nextBatch(benchmark);

			BatchCacheSingleton.getInstance().addToQueue(batch);

			// !TODO: set the size in a config file.
			if (BatchCacheSingleton.getInstance().size() > Constants.BATCH_CACHE_SIZE) {

				try {
					Thread.sleep(1);
//					notifyAll();

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			// Stop when we get the last batch
			if (batch.getLast()) {
				System.out.println("Received lastbatch on the Reader Side, Wait for the consumer ... ");
				break;
			}

			cnt++;
			System.out.println(cnt + " , " + BatchCacheSingleton.getInstance().size());

		}

	}

}
