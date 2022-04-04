# Debs2022

## Running locally

### Configuration file
File `cloud.properties` contains `host:port` definitions for each worker.  
Workers will automatically find their configuration in the properties file.

### Running workers
For each worker process, run:
```shell
gradlew runWorker
```

