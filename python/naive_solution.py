from challenge_benchmark import Benchmark
from typing import Dict, List, Tuple
from collections import defaultdict
from queue import Queue
from google.protobuf.timestamp_pb2 import Timestamp

import messages.challenger_pb2 as ch
import threading

def max_events(e1: ch.Event, e2: ch.Event) -> ch.Event:
    if e2 is None or e1.last_trade.seconds > e2.last_trade.seconds:
        return e1
    
    if e1.last_trade.seconds == e2.last_trade.seconds:
        return e1 if e1.last_trade.nanos >= e2.last_trade.nanos else e2
    
    return e2

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


def batch_processor(benchmark: Benchmark, queue: Queue):
    trackers = {}
    start_time = 0

    # batch_num = 0

    while True:
        batch = queue.get(block=True)
        # print(batch_num)
        # batch_num += 1
        
        for e in batch.events:
            if start_time == 0:
                # set start time
                start_time = e.last_trade.seconds

            if e.symbol not in trackers:
                trackers[e.symbol] = Tracker(e.symbol, start_time)

            tracker = trackers[e.symbol]
            tracker.eval_event(e)

        q1_indicators = list()
        all_crossovers = list()

        for symbol in batch.lookup_symbols:
            if symbol not in trackers:
                continue

            tracker = trackers[symbol]
            
            indicator, crossovers = tracker.get_results()
            
            q1_indicators.append(indicator)
            all_crossovers.extend(crossovers)

        benchmark.submit_q1(
            batch_id=batch.seq_id, indicators=q1_indicators
        )

        benchmark.submit_q2(batch_id=batch.seq_id, crossover_events=all_crossovers)
        
        queue.task_done()
    

def main():
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="new method - david",
        benchmark_type="test",
    )
    
    queue = Queue(maxsize=30)

    threading.Thread(target=batch_processor, daemon=True, args=(benchmark, queue)).start()
    
    for batch in benchmark.get_batches():
        queue.put(batch, block=True)
        # print("size:", queue.qsize())

if __name__ == "__main__":
    main()
