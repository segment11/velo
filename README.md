# Velo

Velo (pronunciation: /vɪloʊ/) is a redis protocol compatible, low latency, hash index based, slot sharding,
multi-threading key value storage system.

## Redis command support

Velo supports redis main data types:

- string
- hash
- list
- set
- zset
- bitmap
- hyperloglog
- bloomfilter
- geo

Details refer to https://github.com/segment11/velo/blob/main/doc/redis_command_support.md

## Ingestion

- parquet file
- sst file [todo]

## Get started

### Build

Prepare the environment:

- JDK 21
- Gradle 8.x

```shell
git clone https://github.com/segment11/velo.git
cd velo
gradle jar
```

### Run

Change build/libs/velo.properties:

```properties
slotNumber=1
netWorkers=1
dir=/tmp/velo-data
net.listenAddresses=127.0.0.1:7379
estimateKeyNumber=10000000
kv.lru.maxSize=1000000
```

```shell
cd build/libs
java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

### Test

```shell
redis-cli -p 7379
get key
set key value
```

### Benchmark

```shell
redis-benchmark -p 7379 -c 1 -t set -d 200 -n 10000000 -r 10000000
```

### Performance

Details refer to https://github.com/segment11/velo/blob/main/doc/performance

## Contributing

If you are interested in contributing to Velo, please refer to [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Velo is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE-2.0.txt) for the full license text.

## Acknowledgements

Velo uses the following open source projects:

- [ActiveJ](https://activej.io/)
- [zstd-jni](https://github.com/luben/zstd-jni)
- [Apache Curator](https://curator.apache.org/)
- [Spock](https://spockframework.org/)
