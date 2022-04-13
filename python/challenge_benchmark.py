import grpc
import messages.challenger_pb2 as ch
import messages.challenger_pb2_grpc as api
import threading

from typing import Optional


op = [
    ("grpc.max_send_message_length", 10 * 1024 * 1024),
    ("grpc.max_receive_message_length", 100 * 1024 * 1024),
]


class Benchmark:
    def __init__(
        self,
        token: str,
        benchmark_name: str,
        benchmark_type: str,
        grpc_host: Optional[str]='challenge.msrg.in.tum.de',
        grpc_port: Optional[str]='5023',
    ):
        self.config = ch.BenchmarkConfiguration(
            token=token,
            benchmark_name=benchmark_name,
            benchmark_type=benchmark_type,
            queries=[ch.Query.Q1, ch.Query.Q2],
        )
        self.channel = grpc.insecure_channel(
            f'{grpc_host}:{grpc_port}', options=op
        )
        self.stub = api.ChallengerStub(self.channel)
        self.benchmark = self.stub.createNewBenchmark(self.config)
        self.started = False
        self.ended = False
        self.lock = threading.Lock()

    def start(self):
        # Do nothing if already started.
        if self.started:
            return

        self.started = True
        self.ended = False
        self.stub.startBenchmark(self.benchmark)

    def stop(self):
        self.stub.endBenchmark(self.benchmark)

    def next_batch(self):
        return self.stub.nextBatch(self.benchmark)

    @property
    def id(self):
        return self.benchmark.id

    def submit_q1(self, batch_id: str, indicators: str):
        result = ch.ResultQ1(
            benchmark_id=self.id, batch_seq_id=batch_id, indicators=indicators
        )
        self.stub.resultQ1(result)

    def submit_q2(self, batch_id, crossover_events):
        result = ch.ResultQ2(
            benchmark_id=self.id,
            batch_seq_id=batch_id,
            crossover_events=crossover_events,
        )
        self.stub.resultQ2(result)

    def has_next(self):
        self.lock.acquire()
        if not self.ended:
            return True
        self.lock.release()
        return False

    def next(self):
        batch = self.next_batch()
        if batch.last:
            self.ended = True
        self.lock.release()
        return batch

