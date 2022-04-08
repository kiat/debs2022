from challenge_benchmark import Benchmark
from typing import Dict, List, Tuple
from collections import defaultdict
from queue import Queue
from google.protobuf.timestamp_pb2 import Timestamp

import messages.challenger_pb2 as ch
import threading

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

            cur_38 = ema_j(event.last_trade_price, self.prev_ema_38, 38)
            cur_100 = ema_j(event.last_trade_price, self.prev_ema_100, 100)

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
    
    def get_results(self) -> Tuple[ch.Indicator, List[ch.CrossoverEvent]]:
        return ch.Indicator(symbol= self.symbol, ema_38=self.prev_ema_38, ema_100=self.prev_ema_100), self.crossovers
    

class ProcessEvents (threading.Thread):
    def __init__(self, trackers, events, start_time):
        threading.Thread.__init__(self)
        self.trackers = trackers
        self.events = events
        self.start_time = start_time

    def run(self):
        for e in self.events:
            if e.symbol not in self.trackers:
                self.trackers[e.symbol] = Tracker(e.symbol, self.start_time)

            tracker = self.trackers[e.symbol]
            tracker.eval_event(e)


def batch_processor(benchmark: Benchmark, queue: Queue):
    trackers = {}
    
    batch_num = 0
    

    while True:
        list_of_events, lookup_symbols, seq_id, start_time = queue.get(block=True)
        print(batch_num)
        batch_num += 1

        num_threads = len(list_of_events)

        threads = list()

        for i in range(num_threads):
            threads.append((ProcessEvents(trackers, list_of_events[i], start_time)))

        for i in range(num_threads):
            threads[i].start()
        
        for i in range(num_threads):
            threads[i].join()

        q1_indicators = list()
        all_crossovers = list()

        for symbol in lookup_symbols:
            if symbol not in trackers:
                continue

            tracker = trackers[symbol]
            
            indicator, crossovers = tracker.get_results()
            
            q1_indicators.append(indicator)
            all_crossovers.extend(crossovers)

        benchmark.submit_q1(
            batch_id=seq_id, indicators=q1_indicators
        )

        benchmark.submit_q2(batch_id=seq_id, crossover_events=all_crossovers)
        
        queue.task_done()

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

# split a batch in the num_thread number of lists 
def split_batch(batch, num_threads, start_time):
    list_of_events = list()

    for i in range(num_threads):
        list_of_events.append(list())

    for e in batch.events:
        index = hash(e.symbol) % num_threads
        list_of_events[index].append(e)

    return list_of_events, batch.lookup_symbols, batch.seq_id, start_time


class ProcessBatches (threading.Thread):
    def __init__(self, benchmark, counter, queue, start_time, num_consumers):
        threading.Thread.__init__(self)
        self.benchmark = benchmark
        self.counter = counter
        self.queue = queue
        self.start_time = start_time
        self.num_consumers = num_consumers        

    def run(self):
        while self.benchmark.has_next():
            batch = self.benchmark.next()
            obj = split_batch(batch, self.num_consumers, self.start_time)
            
            # wait for counter to equal batch num 
            while not self.counter.is_value(obj[2]):
                pass

            # print("batch num: ", obj[2], flush=True)
            # at this point counter == batch_num
            self.queue.put(obj, block=True)

            # increment counter so next batch can be put into the queue
            self.counter.increment()



def main():
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="new method - david",
        benchmark_type="test",
    )
    
    queue = Queue(maxsize=30)

    consumer_main = threading.Thread(target=batch_processor, daemon=True, args=(benchmark, queue))
    consumer_main.start()

    num_producers = 2
    num_consumers = 4
    start_time = 0
    
    benchmark.start()

    benchmark.has_next()
    batch = benchmark.next()
    start_time = batch.events[0].last_trade.seconds

    queue.put(split_batch(batch, num_consumers, start_time), block=True)

    threads = list()
    counter = Counter()

    for i in range(num_producers):
        threads.append((ProcessBatches(benchmark, counter, queue, start_time, num_consumers)))

    for i in range(num_producers):
        threads[i].start()
    
    for i in range(num_producers):
        threads[i].join()
    
    queue.join()

    print("stop benchmark")
    benchmark.stop()

if __name__ == "__main__":
    main()
