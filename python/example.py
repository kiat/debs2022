import os
import logging
from datetime import datetime

# If grpc is missing: pip install grpcio
import grpc
from google.protobuf import empty_pb2

# If the classes below are missing, generate them:
# You need to install the grpcio-tools to generate the stubs: pip install grpcio-tools
# python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. challenger.proto
import challenger_pb2 as ch
import challenger_pb2_grpc as api

import challenger_pb2 as ch
import challenger_pb2_grpc as api

op = [('grpc.max_send_message_length', 10 * 1024 * 1024),
      ('grpc.max_receive_message_length', 100 * 1024 * 1024)]
with grpc.insecure_channel('challenge.msrg.in.tum.de:5023', options=op) as channel:
    stub = api.ChallengerStub(channel)


    #Step 1 - Create a new Benchmark
    benchmarkconfiguration = ch.BenchmarkConfiguration(token='zqultcyalnowfgxjlzlsztkcquycninr'.strip(),
                                                       batch_size=3_000,
                                                       benchmark_name="this name shows_up_in_dashboard",
                                                       benchmark_type="test",
                                                       queries=[ch.BenchmarkConfiguration.Query.Q1, ch.BenchmarkConfiguration.Query.Q2])
    benchmark = stub.createNewBenchmark(benchmarkconfiguration)

    #Step 2 - get all locations
    # The locations depend on the benchmark_type
    loc = stub.getLocations(benchmark) #get all the locations
    print("Fetched %s locations" % (len(loc.locations)))


    #Step 3 (optional) - Calibrate the latency
    ping = stub.initializeLatencyMeasuring(benchmark)
    for i in range(10):
        ping =stub.measure(ping)
    stub.endMeasurement(ping)

    #Step 4 - Start Eventprocessing and start the clock
    stub.startBenchmark(benchmark)
    # The clock is now ticking ...

    cnt_current = 0
    cnt_historic = 0
    cnt = 0

    batch = stub.nextBatch(benchmark)
    while batch:
        cnt_current += len(batch.current)
        cnt_historic += len(batch.lastyear)

        if(cnt % 100) == 0:
            ts_str = ""
            if len(batch.current) > 0: #it could happen that only events from lastyear are available
                ts = batch.current[0].timestamp
                dt = datetime.utcfromtimestamp(ts.seconds)
                ts_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")

            print("processed %s - current_time: %s, num_current: %s, num_historic: %s, total_events: %s" % (cnt, ts_str, cnt_current, cnt_historic, ( cnt_current + cnt_historic)))


        # result_payload_q1 = processTheBatchQ1(batch) #here is your implementation ;)
        resultQ1 = ch.ResultQ1(benchmark_id=benchmark.id,  #The id of the benchmark
                               batch_seq_id=batch.seq_id,
                               topkimproved=[])

        stub.resultQ1(resultQ1) #send the result of query 1, also send the result of Q2 in case you calculate both

        # processTheBatchQ1(batch) # here should be the implementation of Q2
        resultQ2 = ch.ResultQ2(benchmark_id=benchmark.id, batch_seq_id=batch.seq_id, histogram=[])
        stub.resultQ2(resultQ2)

        if batch.last or cnt > 1_000: #here we just stop after 1000 so we see a result, this is allowed only for testing
            break

        cnt = cnt + 1
        batch = stub.nextBatch(benchmark)

    stub.endMeasurement(benchmark)

print("finished")
