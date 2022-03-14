from challenge_benchmark import Benchmark
from typing import Dict, List, Tuple
from collections import defaultdict

import messages.challenger_pb2 as ch


def compute_ema(
        events: List[ch.Event],
        previous: Dict[str, List[float]]
    ) -> Tuple[List[ch.Indicator], Dict[str, List[float]]]:
    """
    Computes the EMAs for the current batch/window and returns the list of indicators 
    + updated dictionary.

    Args:
        events (List[ch.Event]): Events from batch.
        previous (Dict[str, List[float]]): EMAs previous previous time window (i - 1).

    Returns:
        List[ch.Indicator]: List of indicators.
        Dict[str, List[float]]: Updated EMAs for current time window. First element is j = 38, 
                                second element is j = 100.
    """
    
    weighted_first = lambda closing, j: closing * (2 / (1 + j))
    weighted_second = lambda prev_w, j: prev_w * (1 - (2 / (1 + j)))
    ema_j = lambda closing, prev_w, j: weighted_first(closing, j) + weighted_second(prev_w, j)

    cur = defaultdict(list)
    indicators = []
    for event in events:
        symbol = event.symbol

        prev_38, prev_100 = 0, 0 if symbol not in previous else previous[symbol]
        
        ema_38 = ema_j(event.last_trade_price, prev_38, 38)
        ema_100 = ema_j(event.last_trade_price, prev_100, 100)
        
        indicators.append(ch.Indicator(symbol=event.symbol, ema_38=ema_38, ema_100=ema_100))
        
        cur[symbol] = [ema_38, ema_100]
    return indicators, cur
            


def crossover_events(
        symbols: List[str],
        cur: Dict[str, List[float]],
        prev: Dict[str, List[float]],
        events: List[ch.Event]
    ) -> List[ch.CrossoverEvent]:
    """Detects cross over events: bullish or bearish based on two EMAs with j: 38, 100.

    Args:
        symbols (List[str]): Financial instrument symbols for this batch/window.
        cur (Dict[List[float]]): EMAs for window i
        prev (Dict[List[float]]): EMAs for window i - 1

    Returns:
        List[ch.CrossoverEvent]: List of crossover events (signal type, security type, timestamp, symbol).
    """
    
    crossovers = []
    for s in symbols:
        
        # No event if not in previous or current window.
        if s not in prev or s not in cur:
            continue
        
        prev_w = prev[s]
        cur_w = cur[s]

        # Trigger buy.
        if prev_w[0] <= prev_w[1] and cur_w[0] > cur_w[1]:
            pass
        
        # TODO: get security types without going through all events again.
    
    return crossovers
    
    
    


def main():
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="Naive solution",
        benchmark_type="test",
    )

    event_count = 0
    batch_count = 0
    
    previous = defaultdict(list)
    
    for batch in benchmark.get_batches():
        batch_size = len(batch.events)

        print(f"Batch [num={batch_count} size={batch_size}]")
        event_count += batch_size
        batch_count += 1

        indicators, cur = compute_ema(batch.events, previous)
        benchmark.submit_q1(
            batch_id=batch.seq_id, indicators=indicators
        )
        
        crossovers = crossover_events(batch.lookup_symbols, cur, previous)
        benchmark.submit_q2(batch_id=batch.seq_id, crossover_events=crossover_events())
        
        benchmark.stop()

if __name__ == "__main__":
    main()
