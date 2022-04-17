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

**Options**:
```shell
python3 solution.py --help
```
Able to tune the number of producers, consumers, and the queue size for the producers and consumers.
