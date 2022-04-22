from challenge_benchmark import Benchmark
from typing import Dict, List, Optional, Tuple
from queue import Queue

import messages.challenger_pb2 as ch
import threading
import argparse
import logging


def get_crossover(
        ema_38: float,
        ema_100: float,
        cur_38: float,
        cur_100: float,
        e: ch.Event
    ) -> Optional[ch.CrossoverEvent]:
    """ Helper function for determining crossovers.

    Args:
        ema_38 (float): The previous EMA38 value.
        ema_100 (float): The previous EMA100 value.
        cur_38 (float): The current EMA38 value.
        cur_100 (float): The current EMA100 value.
        e (ch.Event): Event.

    Returns:
        Optional[ch.Crossover]: A crossover event.
    """
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
    """ Tracks latest EMAs and crossover events on a per-symbol basis. """

    def __init__(self, symbol: str, reference: int) -> None:
        """ Initializes the tracker.

        Args:
            symbol (str): Symbol to track.
            reference (int): Reference time to use for calculating EMAs.
        """
        self.symbol = symbol
        self.start_time = reference
        self.prev_ema_38 = 0
        self.prev_ema_100 = 0
        self.latest_event = None
        self.crossovers = list()
   
    def eval_event(self, event: ch.Event) -> None:
        """ Evaluates an event and updates the tracker's EMAs or crossovers.

        Args:
            event (ch.Event): Event to evaluate.
        """

        # index is 0 which means current window is still open
        # or index is > 0 which means current window has now closed.
        index = (event.last_trade.seconds - self.start_time) // (5 * 60)

        # assumption is that events are loosely chronological, hence we update
        # the latest event with the last event that we have seen.
        if index == 0:
            self.latest_event = event
        else:
            # in this case, we've received an event for the next window. we can now
            # close the current window and update the EMAs.

            
            # ema calculations.
            weighted_first = lambda closing, j: closing * (2 / (1 + j))
            weighted_second = lambda prev_w, j: prev_w * (1 - (2 / (1 + j)))
            ema_j = lambda closing, prev_w, j: weighted_first(closing, j) + weighted_second(prev_w, j)

            last_trade_price = 0 if self.latest_event is None else self.latest_event.last_trade_price
            cur_38 = ema_j(last_trade_price, self.prev_ema_38, 38)
            cur_100 = ema_j(last_trade_price, self.prev_ema_100, 100)

            # detect crossovers.
            crossover = get_crossover(
                self.prev_ema_38,
                self.prev_ema_100,
                cur_38,
                cur_100,
                self.latest_event
            )

            if crossover is not None:
                self.crossovers.append(crossover)
                while (len(self.crossovers) > 3):
                    self.crossovers.pop(0)

            # update prev ema to be the ema of the closed window.
            self.prev_ema_38 = cur_38
            self.prev_ema_100 = cur_100

            # update start time to be the start of the new window.
            self.start_time += index * (5 * 60)
            self.latest_event = event
            
    def get_results(self) -> Tuple[ch.Indicator, List[ch.CrossoverEvent]]:
        """ Returns the latest EMAs and crossovers for this symbol's queries.

        Returns:
            Tuple[ch.Indicator, List[ch.CrossoverEvent]]: Latest EMAs and crossovers.
        """
        return ch.Indicator(
            symbol=self.symbol, 
            ema_38=self.prev_ema_38,
            ema_100=self.prev_ema_100
        ), self.crossovers
    
# ------------------------------------------------- CONSUMERS -------------------------------------------------

class ProcessEvents (threading.Thread):
    """ Consumer thread to process events for a set of allocated symbols. """

    def __init__(self, trackers: Dict[str, Tracker], events: List[ch.Event], start_time: int) -> None:
        """ Initializes the consumer thread.

        Args:
            trackers (Dict[str, Tracker]): Trackers to use.
            events (List[ch.Event]): Events to process.
            start_time (int): Start time for window reference.
        """
        threading.Thread.__init__(self)
        self.trackers = trackers
        self.events = events
        self.start_time = start_time

    def run(self):
        """ Processes symbols' events. """

        for e in self.events:
            
            # Add tracker if not present.
            if e.symbol not in self.trackers:
                self.trackers[e.symbol] = Tracker(e.symbol, self.start_time)

            tracker = self.trackers[e.symbol]
            tracker.eval_event(e)

def submit_results(
        benchmark: Benchmark,
        seq_id: int,
        q1_indicators: List[ch.Indicator],
        all_crossovers: List[ch.CrossoverEvent]
    ) -> None:
    """ Submits results to the benchmark.

    Args:
        benchmark (Benchmark): Benchmark to submit to.
        seq_id (int): Batch sequential ID.
        q1_indicators (List[ch.Indicator]): Q1 indicators.
        all_crossovers (List[ch.CrossoverEvent]): Q2 crossovers.
    """
    benchmark.submit_q1(batch_id=seq_id, indicators=q1_indicators)
    benchmark.submit_q2(batch_id=seq_id, crossover_events=all_crossovers)

def batch_processor(benchmark: Benchmark, queue: Queue):
    """ Consumer manager -- starts and stops consumers.

    Args:
        benchmark (Benchmark): Benchmark to submit to.
        queue (Queue): Queue for batches.
    """
    
    # Global tracker for all symbols.
    trackers = {}
    
    batch_num = 0
    
    logger = logging.getLogger(__name__)
    
    while True:
        
        # Pop off queue (single batch at a time).
        list_of_events, lookup_symbols, seq_id, start_time = queue.get(block=True)
        logger.info(f"Processing batch {seq_id}")
        batch_num += 1

        
        # Starting consumers.
        num_threads = len(list_of_events)

        threads = list()
        for i in range(num_threads):
            threads.append((ProcessEvents(trackers, list_of_events[i], start_time)))

        for i in range(num_threads):
            threads[i].start()
        
        # Wait for consumers to finish.
        for i in range(num_threads):
            threads[i].join()

        # Submit final results.
        q1_indicators = list()
        all_crossovers = list()

        for symbol in lookup_symbols:

            if symbol not in trackers:
                continue

            tracker = trackers[symbol]

            indicator, crossovers = tracker.get_results()

            q1_indicators.append(indicator)
            all_crossovers.extend(crossovers)


        threading.Thread(
            target=submit_results,
            daemon=True,
            args=(benchmark, seq_id, q1_indicators, all_crossovers)
        ).start()
        
        queue.task_done()

# ------------------------------------------------- PRODUCERS -------------------------------------------------

class Counter:
    """ Class used to ensure that pre-processed batches are put into the queue in the correct order. """
    def __init__(self) -> None:
        """ Initializes counter (set to 1 for first batch)."""
        
        self.value = 1
        self.lock = threading.Lock()
        self.event = threading.Event()

    def is_value(self, num: int) -> bool:
        """ Checks if the current value is equal to the given value.

        Args:
            num (int): Value to check.

        Returns:
            bool: True if equal, False otherwise.
        """
        if self.value != num:
            self.event.wait()
            return False  
        return True

    def increment(self) -> None:
        """ Atomically increment the value."""
        with self.lock:
            self.value += 1
            self.event.set()
            self.event.clear()


def split_batch(batch: ch.Batch, num_threads: int, start_time: int) -> Tuple[List[ch.Batch], List[str], int, int]:
    """ Splits a batch into a list of batches. For pushing into the queue.

    Args:
        batch (ch.Batch): Batch to split.
        num_threads (int): Number of consumers.
        start_time (int): Reference time for starting.

    Returns:
        Tuple[List[ch.Batch], List[str], int, int]: Split events, lokup symbols, batch sequential ID, start time.
    """
    list_of_events = [[] for _ in range(num_threads)]

    # Hash events to consumers.
    for e in batch.events:
        index = hash(e.symbol) % num_threads
        list_of_events[index].append(e)

    return list_of_events, batch.lookup_symbols, batch.seq_id, start_time


class ProcessBatches (threading.Thread):
    """ Producer thread to process batches. """
    def __init__(
        self,
        benchmark: Benchmark,
        counter: Counter,
        queue: Queue,
        start_time: int,
        num_consumers: int
    ) -> None:
        """ Initializes the producer thread.

        Args:
            benchmark (Benchmark): Benchmark to submit to.
            counter (Counter): Counter to synchronize pushing of batches.
            queue (Queue): Queue to push batches to.
            start_time (int): Reference start time.
            num_consumers (int): Number of consumers.
        """
        threading.Thread.__init__(self)
        self.benchmark = benchmark
        self.counter = counter
        self.queue = queue
        self.start_time = start_time
        self.num_consumers = num_consumers        

    def run(self):
        """ Processes batches by pushing onto the queue. """

        while self.benchmark.has_next():
            batch = self.benchmark.next()
            obj = split_batch(batch, self.num_consumers, self.start_time)
            
            # wait for counter to equal batch num 
            while not self.counter.is_value(obj[2]):
                pass

            # at this point counter == batch_num
            self.queue.put(obj, block=True)

            # increment counter so next batch can be put into the queue
            self.counter.increment()



def main():

    Log_Format = "%(levelname)s %(asctime)s - %(message)s"

    # Argument parsing.
    parser = argparse.ArgumentParser(description='DEBS implementation.')
    parser.add_argument('--num_consumers', type=int, default=4, help='Number of consumers.')
    parser.add_argument('--num_producers', type=int, default=4, help='Number of producers.')
    parser.add_argument('--queue_size', type=int, default=30, help='Queue size for batches.')
    parser.add_argument('--name', type=str, default='DEBS', help='Name of benchmark.')
    
    args = parser.parse_args()
    
    CONSUMERS = args.num_consumers
    PRODUCERS = args.num_producers
    QUEUE_SIZE = args.queue_size

    logging.basicConfig(
        filename = f'{args.name}.log',
        filemode = "w",
        format = Log_Format, 
        level = logging.INFO
    )

    logger = logging.getLogger(__name__)

    logger.info(f"Starting {PRODUCERS} producers and {CONSUMERS} consumers.")

    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name=args.name,
        benchmark_type="test",
    )

    queue = Queue(maxsize=QUEUE_SIZE)

    consumer_main = threading.Thread(target=batch_processor, daemon=True, args=(benchmark, queue))
    consumer_main.start()

    num_producers = PRODUCERS
    num_consumers = CONSUMERS
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

    benchmark.stop()

if __name__ == "__main__":
    main()
