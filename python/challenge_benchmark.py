import grpc
import challenger_pb2 as ch
import challenger_pb2_grpc as api


op = [
    ("grpc.max_send_message_length", 10 * 1024 * 1024),
    ("grpc.max_receive_message_length", 100 * 1024 * 1024),
]


class Benchmark:
    def __init__(self, token, benchmark_name, benchmark_type):
        self.config = ch.BenchmarkConfiguration(
            token=token,
            benchmark_name=benchmark_name,
            benchmark_type=benchmark_type,
            queries=[ch.Query.Q1, ch.Query.Q2],
        )
        self.channel = grpc.insecure_channel(
            "challenge.msrg.in.tum.de:5023", options=op
        )
        self.stub = api.ChallengerStub(self.channel)
        self.benchmark = self.stub.createNewBenchmark(self.config)

    def start(self):
        self.stub.startBenchmark(self.benchmark)

    def stop(self):
        self.stub.endBenchmark(self.benchmark)

    def next_batch(self):
        return self.stub.nextBatch(self.benchmark)

    @property
    def id(self):
        return self.benchmark.id

    def submit_q1(self, batch_id, indicators):
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
