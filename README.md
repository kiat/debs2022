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
