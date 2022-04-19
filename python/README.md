# Debs2022

## Running locally
Change into the `python` directory.

**Install the required packages**:

```shell
pip3 install grpcio
pip install grpcio-tools
```

Or use:
```shell
pip3 install -r requirements.txt
```

**Create the Protobuf messages**:
```shell
make messages
```
**Running the code**:
```shell
python3 solution.py
```

Run it with different number of Event Consumer and Producers and queue size based on avaialble CPU and RAM. 


```shell
python3 solution.py  --num_producers 4 --num_consumers 16 --queue_size 10 
```



**Options**:
```shell
python3 solution.py --help
```
Able to tune the number of producers, consumers, and the queue size for the producers and consumers.


# Documentations from Organizers. 



### Step 0
Generate the client code, as example, we use Python. For other languages check the documentation

```   
# Install dependencies
pip install grpcio
pip install grpcio-tools
```

### Generate client code

```
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. challenger.proto
```
    
### Step 0
Initilize the client stub from the generated code

```     
import challenger_pb2 as ch
import challenger_pb2_grpc as api

op = [('grpc.max_send_message_length', 10 * 1024 * 1024),
      ('grpc.max_receive_message_length', 100 * 1024 * 1024)]
with grpc.insecure_channel('challenge.msrg.in.tum.de:5023', options=op) as channel:
    stub = api.ChallengerStub(channel)
```     
    
### Step 1
Create a new Benchmark. You have set your token (see profile), set a benchmark name (this is only shown in your statistics) and the batchsize. The benchmark_type should be set to "test" if you experiment. Also add the list of which queries you want to run, either just one, [Q1] or [Q2] or both at the same time [Q1, Q2].

```     
benchmarkconfiguration = ch.BenchmarkConfiguration(
                token="get profile", #The api token is available in the profile, see here: https://challenge.msrg.in.tum.de/profile/
                benchmark_name="shows_up_in_dashboard", #This name is used here: https://challenge.msrg.in.tum.de/benchmarks/
                benchmark_type="test", #Test or Evaluation, Evaluation will be available end of January. Test can be used to start implementing
                queries=[ch.Query.Q1, ch.Query.Q2])
benchmark = stub.createNewBenchmark(benchmarkconfiguration)

```     
    
### Step 2
First start the Benchmark. This sets the timestamp server side for the throughput measurments. Then process all the batches. The batches are correlated for the latency measurements.

Once you called the endBenchmark RPC, we calculate the results. For testing, you can call endBenchmark early, e.g., after 100 batches.

```     
stub.startBenchmark(benchmark)
```     
    
### Step 3
Start processing events!

```     
while True:
    batch = stub.nextBatch(benchmark)
    event_count = event_count + len(batch.events)

    def queryResults(symbols:list[str]) -> list[ch.Indicator]:
        # Your part: calculate the indicators for the given symbols
        return list()

    resultQ1 = ch.ResultQ1(
        benchmark_id=benchmark.id, #The id of the benchmark
        batch_seq_id=batch.seq_id, #The sequence id of the batch
        indicators=queryResults(batch.lookup_symbols))
    stub.resultQ1(resultQ1)  # send the result of query 1 back
    
    def crossoverEvents() -> list[ch.CrossoverEvent]:
        Your part: calculate the crossover events
        return list()

    # do the same for Q2
    resultQ2 = ch.ResultQ2(
        benchmark_id=benchmark.id, #The id of the benchmark
        batch_seq_id=batch.seq_id, #The sequence id of the batch
        crossover_events=crossoverEvents()) 
    
    stub.resultQ2(resultQ2) # submit the results of Q2
    
    # Step 4 - once the last event is received, stop the clock
    # See the statistics within ~5min here: https://challenge.msrg.in.tum.de/benchmarks/
    if batch.last:
        print(f"received last batch, total batches: {event_count}")
        stub.endBenchmark(benchmark)
        break
        
```
