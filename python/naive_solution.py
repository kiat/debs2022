from challenge_benchmark import Benchmark
import challenger_pb2 as ch


def compute_ema(symbols: list[str]) -> list[ch.Indicator]:
    # TODO: naive implementation
    return list()


def crossover_events() -> list[ch.CrossoverEvent]:
    # TODO: naive implementation
    return list()


def main():
    benchmark = Benchmark(
        token="zqultcyalnowfgxjlzlsztkcquycninr",
        benchmark_name="Naive solution",
        benchmark_type="test",
    )

    event_count = 0
    batch_count = 0

    benchmark.start()

    while True:
        batch = benchmark.next_batch()
        batch_size = len(batch.events)

        print(f"Batch [num={batch_count} size={batch_size}]")
        event_count += batch_size
        batch_count += 1

        benchmark.submit_q1(
            batch_id=batch.seq_id, indicators=compute_ema(batch.lookup_symbols)
        )

        benchmark.submit_q2(batch_id=batch.seq_id, crossover_events=crossover_events())

        if batch.last:
            benchmark.stop()
            break


if __name__ == "__main__":
    main()
