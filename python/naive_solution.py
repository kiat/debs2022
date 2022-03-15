import google.protobuf.timestamp_pb2

from challenge_benchmark import Benchmark
import challenger_pb2 as ch


class EMA:
    C38 = 2 / (1 + 38)
    C100 = 2 / (1 + 100)

    def __init__(self, benchmark, w_length):
        self.benchmark = benchmark
        self.w_start = -1
        self.data = dict()
        self.prev_ema38 = dict()
        self.prev_ema100 = dict()
        self.symbols = set()
        self.sec_types = dict()
        self.w_length = w_length

    def reset(self):
        self.w_start = -1
        self.data = dict()
        self.prev_ema38 = dict()
        self.prev_ema100 = dict()
        self.symbols = set()
        self.sec_types = dict()

    @staticmethod
    def get_minutes(timestamp):
        return timestamp.seconds // 60

    @staticmethod
    def compute_ema(close_price, prev_ema, c):
        return close_price * c + prev_ema * (1 - c)

    @staticmethod
    def make_response(msg, **kwargs):
        for k, v in kwargs.items():
            setattr(msg, k, v)
        return msg

    def get_prev_ema38(self, k):
        return self.prev_ema38[k] if k in self.prev_ema38 else 0

    def get_prev_ema100(self, k):
        return self.prev_ema100[k] if k in self.prev_ema100 else 0

    def make_sell(self, k):
        return EMA.make_response(
            ch.CrossoverEvent(),
            ts=EMA.make_response(
                google.protobuf.timestamp_pb2.Timestamp(),
                seconds=(self.w_start + self.w_length) * 60,
                nano=0
            ),
            symbol=k,
            security_type=self.sec_types[k],
            signal_type=ch.CrossoverEvent.SignalType.Sell
        )

    def make_buy(self, k):
        return EMA.make_response(
            ch.CrossoverEvent(),
            ts=EMA.make_response(
                google.protobuf.timestamp_pb2.Timestamp(),
                seconds=(self.w_start + self.w_length) * 60,
                nano=0
            ),
            symbol=k,
            security_type=self.sec_types[k],
            signal_type=ch.CrossoverEvent.SignalType.Buy
        )

    def produce_ema(self, batch_id):
        crossovers = []

        for k, v in self.data.items():
            ema38 = EMA.compute_ema(v, self.get_prev_ema38(k), EMA.C38)
            ema100 = EMA.compute_ema(v, self.get_prev_ema100(k), EMA.C100)

            if k in self.prev_ema100 and k in self.prev_ema38:
                if ema38 < ema100 and self.prev_ema38[k] >= self.prev_ema100[k]:
                    crossovers.append(self.make_sell(k))

                if ema38 > ema100 and self.prev_ema38[k] <= self.prev_ema100[k]:
                    crossovers.append(self.make_buy(k))

            self.prev_ema38[k] = ema38
            self.prev_ema100[k] = ema100

        indicators = map(
            lambda s: EMA.make_response(
                ch.Indicator(),
                symbol=s,
                ema_38=self.prev_ema38[s],
                ema_100=self.prev_ema100[s]
            ),
            self.symbols
        )

        self.benchmark.submit_q1(batch_id=batch_id, indicators=indicators)
        self.benchmark.submit_q2(batch_id=batch_id, crossover_events=crossovers)

    def aggregate(self, batch):
        self.reset()

        for event in batch.events:
            w = EMA.get_minutes(event.last_trade)

            if self.w_start == -1:
                self.w_start = w

            if w > self.w_start + self.w_length:
                self.produce_ema(batch.seq_id)
                self.w_start = self.w_start + self.w_start

            self.symbols.add(event.symbol)
            self.sec_types[event.symbol] = event.security_type
            self.data[event.symbol] = event.last_trade_price


def main():
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="Naive solution",
        benchmark_type="test",
    )

    ema = EMA(benchmark, w_length=5)

    event_count = 0
    batch_count = 0

    benchmark.start()

    while True:
        batch = benchmark.next_batch()
        batch_size = len(batch.events)
        print(f"Batch [num={batch_count} size={batch_size}]")

        ema.aggregate(batch)

        event_count += batch_size
        batch_count += 1

        if batch.last:
            benchmark.stop()
            break


if __name__ == "__main__":
    main()
