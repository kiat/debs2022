from concurrent.futures import thread
from re import sub
from challenge_benchmark import Benchmark
from typing import Dict, List, Tuple
from collections import defaultdict
from queue import Queue
from google.protobuf.timestamp_pb2 import Timestamp

import messages.challenger_pb2 as ch
import threading
import argparse
import time

from threading import Barrier

def get_crossover(ema_38, ema_100, cur_38, cur_100, e):
    type = None
    if ema_38 <= ema_100 and cur_38 > cur_100:
        type = ch.CrossoverEvent.SignalType.Buy
    elif ema_38 >= ema_100 and cur_38 < cur_100:
        type = ch.CrossoverEvent.SignalType.Sell

    if type is not None and e is not None:
        return ch.CrossoverEvent(
                ts=e.last_trade,
                symbol=e.symbol,
                security_type=e.security_type,
                signal_type=type
            )
    
    return None

class Tracker:
    def __init__(self, symbol: str, reference: int) -> None:
        self.symbol = symbol
        self.start_time = reference
        self.prev_ema_38 = 0
        self.prev_ema_100 = 0
        self.latest_event = None
        self.crossovers = list()
    
    def eval_event(self, event: ch.Event) -> None:
        # index is 0 which means current window is still open
        # or index is > 0 which means current window has now closed
        index = (event.last_trade.seconds - self.start_time) // (5 * 60)

        if index == 0:
            self.latest_event = event
        else:
            weighted_first = lambda closing, j: closing * (2 / (1 + j))
            weighted_second = lambda prev_w, j: prev_w * (1 - (2 / (1 + j)))
            ema_j = lambda closing, prev_w, j: weighted_first(closing, j) + weighted_second(prev_w, j)

            last_trade_price = self.latest_event.last_trade_price if self.latest_event else 0
            cur_38 = ema_j(last_trade_price, self.prev_ema_38, 38)
            cur_100 = ema_j(last_trade_price, self.prev_ema_100, 100)

            # detect crossovers
            crossover = get_crossover(self.prev_ema_38, self.prev_ema_100, cur_38, cur_100, self.latest_event)
            if crossover is not None:
                self.crossovers.append(crossover)
                while (len(self.crossovers) > 3):
                    self.crossovers.pop(0)

            # update prev ema to be the ema of the closed window
            self.prev_ema_38 = cur_38
            self.prev_ema_100 = cur_100

            # update start time to be the start of the new window
            self.start_time += index * (5 * 60)
            self.latest_event = event
    
    def get_results(self) -> Tuple[ch.Indicator, List[ch.CrossoverEvent]]:
        return ch.Indicator(symbol= self.symbol, ema_38=self.prev_ema_38, ema_100=self.prev_ema_100), self.crossovers

def submit_results(benchmark, seq_id, q1_indicators, all_crossovers):
    benchmark.submit_q1(batch_id=seq_id, indicators=q1_indicators)
    benchmark.submit_q2(batch_id=seq_id, crossover_events=all_crossovers)

# class used to ensure that pre-processed batches are put into the queue in the correct order
class Counter:
    def __init__(self) -> None:
        self.value = 1
        self.lock = threading.Lock()
        self.event = threading.Event()

    def is_value(self, num):
        if self.value != num:
            self.event.wait()
            return False  
        return True

    def increment(self):
        with self.lock:
            self.value += 1
            self.event.set()
            self.event.clear()


def split_batch(batch: ch.Batch, start_time: int, num_consumers: int):
    """ Hashes the symbol of each batch event and places it into the correct queue

    Args:
        batch (ch.Batch): batch to be placed into the queue
        start_time (int): start time of the first event (for alignment)
        num_consumers (int): number of consumers
    """
    
    # TODO: probably get rid of start time.
    events = [ [] for _ in range(num_consumers) ]
    for e in batch.events:
        index = hash(e.symbol) % num_consumers
        events[index].append(e)
    
    symbols = [ [] for _ in range(num_consumers) ]
    for symbol in batch.lookup_symbols:
        index = hash(symbol) % num_consumers
        symbols[index].append(symbol)

    return events, symbols, batch.seq_id, start_time


class ProcessBatches (threading.Thread):
    """ Producer thread (request batches and pushes them to appropriate queue. """
    
    def __init__(self, benchmark: Benchmark, counter: Counter, queues: List[Queue], start_time: int):
        threading.Thread.__init__(self)
        self.benchmark = benchmark
        self.counter = counter
        self.queues = queues
        self.start_time = start_time    

    def run(self):
        while self.benchmark.has_next():
            batch = self.benchmark.next()
            
            split = split_batch(batch, self.start_time, len(self.queues))
            
            # TODO: this can probably be parallelized even more.
            #  wait for counter to equal batch num 
            while not self.counter.is_value(batch.seq_id):
                pass

            # at this point counter == batch_num
            for queue, events, symbols in zip(self.queues, split[0], split[1]):
                queue.put((events, symbols, split[2], split[3]))

            # increment counter so next batch can be put into the queue
            self.counter.increment()
class SubmitQueries (threading.Thread):
    """ Submission thread for batches """
    
    def __init__(self, benchmark: Benchmark, results: Queue, barrier: Barrier, num_consumers: int):
        threading.Thread.__init__(self)
        self.benchmark = benchmark
        self.results = results
        self.barrier = barrier
        self.consumers = num_consumers
    
    def run(self):
        
        while True:
            returned = [ self.results.get(block=True) for _ in range(self.consumers) ]
            
            # Check that all batch ids are the same.
            # TODO: remove this.
            assert all(x[0] == returned[0][0] for x in returned)
            
            print(returned[0][0])

            flatten_q1 = [ indicator for tup in returned for indicator in tup[1] ]
            flatten_q2 = [ crossover for tup in returned for crossover in tup[2] ]
            
            submit_results(self.benchmark, returned[0][0], flatten_q1, flatten_q2)
            

            # Signal that tasks are done.
            for _ in range(self.consumers):
                self.results.task_done()

            self.barrier.wait()
            
class ProcessEvents (threading.Thread):
    """ Consumer thread (processes batches from queue) """

    def __init__(self, trackers: Dict[str, Tracker], queue: Queue, results: Queue, start_time: int, barrier: Barrier):
        threading.Thread.__init__(self)
        self.trackers = trackers
        self.queue = queue
        self.start_time = start_time
        self.results = results
        self.barrier = barrier

    def run(self):
        
        while True:
            list_of_events, lookup_symbols, seq_id, _ = self.queue.get(block=True)

            for e in list_of_events:
                if e.symbol not in self.trackers:
                    self.trackers[e.symbol] = Tracker(e.symbol, self.start_time)
                tracker = self.trackers[e.symbol]
                tracker.eval_event(e)

            q1_indicators = list()
            all_crossovers = list()

            for symbol in lookup_symbols:
                if symbol not in self.trackers:
                    continue

                tracker = self.trackers[symbol]
            
                indicator, crossovers = tracker.get_results()
            
                q1_indicators.append(indicator)
                all_crossovers.extend(crossovers)
            
            self.results.put((seq_id, q1_indicators, all_crossovers))
        
            self.queue.task_done()
            
            # Wait until all threads are done before moving on to next batch.
            self.barrier.wait()


def main():
    
    # Argument parsing.
    parser = argparse.ArgumentParser(description='DEBS implementation.')
    parser.add_argument('--num_consumers', type=int, default=4, help='Number of consumers.')
    parser.add_argument('--num_producers', type=int, default=4, help='Number of producers.')
    parser.add_argument('--queue_size', type=int, default=30, help='Queue size for batches.')
    
    args = parser.parse_args()
    
    CONSUMERS = args.num_consumers
    PRODUCERS = args.num_producers
    QUEUE_SIZE = args.queue_size

    # Begin benchmark.
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="consumer queues - kevin",
        benchmark_type="test",
    )
    
    # Individual queue for each consumer
    queues = [ Queue(maxsize=QUEUE_SIZE) for _ in range(CONSUMERS) ]
    

    benchmark.start()

    # Get the first event for start time.
    start_time = 0
    benchmark.has_next()
    batch = benchmark.next()
    start_time = batch.events[0].last_trade.seconds

    # Split/place the batch into the queues.
    split = split_batch(batch, start_time, len(queues))

    for queue, events, symbols in zip(queues, split[0], split[1]):
        queue.put((events, symbols, split[2], split[3]))


    # Counter to ensure that batches are processed in order.
    counter = Counter()
    
    # Submission queue.
    to_submit = Queue(maxsize=CONSUMERS + 1)

    # Barrier for submission and simultaneous processing of batches.
    barrier = Barrier(CONSUMERS + 1, timeout=10)


    # -------------------- THREADS --------------------

    # Create and start consumer threads.
    consumers = [ ProcessEvents({}, queue, to_submit, start_time, barrier) for queue in queues ]

    # Create and start producer threads.
    producers = [ ProcessBatches(benchmark, counter, queues, start_time) for _ in range(PRODUCERS) ]
    
    submitter = SubmitQueries(benchmark, to_submit, barrier, CONSUMERS)

    for c in consumers:
        c.start()

    for p in producers:
        p.start()
        
    submitter.start()
    
    
    # Wait for producers to finish.
    for p in producers:
        p.join()

    # Wait for everything to be finished processing.
    queue.join()
    
    # Wait for everything to be submitted.
    to_submit.join()

    # Stop the barrier when all batches have been processed.
    barrier.abort()

    benchmark.stop()

if __name__ == "__main__":
    main()
