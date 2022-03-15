from challenge_benchmark import Benchmark
from typing import Dict, List, Tuple
from collections import defaultdict
from google.protobuf.timestamp_pb2 import Timestamp

import messages.challenger_pb2 as ch

def max_events(e1: ch.Event, e2: ch.Event) -> ch.Event:
    if e1.last_trade.seconds > e2.last_trade.seconds:
        return e1
    
    if e1.last_trade.seconds == e2.last_trade.seconds:
        return e1 if e1.last_trade.nanos >= e2.last_trade.nanos else e2
    
    return e2

class Tracker:
    
    def __init__(self, symbol: str, reference: int) -> None:
        self.symbol = symbol
        self.windows = list()
        self.crossovers = list()
        self.start_time = reference
    
    def eval_event(self, event: ch.Event) -> None:

        index = (event.last_trade.seconds - self.start_time) // (5 * 60)
        if index < len(self.windows):
            # check if event is later than current event in window
            self.windows[index] = max_events(self.windows[index], event)
        else:
            while (index > len(self.windows)):
                e = ch.Event(
                    symbol=self.symbol,
                    security_type=event.security_type,
                    last_trade_price=0.0,
                    last_trade=Timestamp(seconds=0, nanos=0)
                )
                self.windows.append(e)
    
            assert len(self.windows) == index
            self.windows.append(event)

    
    
    def get_results(self) -> Tuple[ch.Indicator, List[ch.CrossoverEvent]]:

        weighted_first = lambda closing, j: closing * (2 / (1 + j))
        weighted_second = lambda prev_w, j: prev_w * (1 - (2 / (1 + j)))
        ema_j = lambda closing, prev_w, j: weighted_first(closing, j) + weighted_second(prev_w, j)
        
        ema_38 = 0
        ema_100 = 0

        crossovers = list()
        
        for event in self.windows:
            # TODO: store EMAs, instead of iterating over everything
            # for every batch.
            cur_38 = ema_j(event.last_trade_price, ema_38, 38)
            cur_100 = ema_j(event.last_trade_price, ema_100, 100)
            
            if ema_38 <= ema_100 and cur_38 > cur_100:
                crossover_event = ch.CrossoverEvent(
                    ts=event.last_trade,
                    symbol=self.symbol,
                    security_type=event.security_type,
                    signal_type=ch.CrossoverEvent.SignalType.Buy
                )
                
                crossovers.append(crossover_event)
            
            if ema_38 >= ema_100 and cur_38 < cur_100:
                crossover_event = ch.CrossoverEvent(
                    ts=event.last_trade,
                    symbol=self.symbol,
                    security_type=event.security_type,
                    signal_type=ch.CrossoverEvent.SignalType.Sell
                )
                
                crossovers.append(crossover_event)

            while len(crossovers) > 3:
                crossovers.pop(0)
            
        return ch.Indicator(symbol= self.symbol, ema_38=ema_38, ema_100=ema_100), crossovers
                



def main():
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="david",
        benchmark_type="test",
    )

    event_count = 0
    batch_count = 0

    trackers = {}
    
    start_time = 0

    for batch in benchmark.get_batches():
        batch_size = len(batch.events)

        print(f"Batch [num={batch_count} size={batch_size}]")
        event_count += batch_size
        batch_count += 1

        # Evaluate all events in batches.
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

if __name__ == "__main__":
    main()
