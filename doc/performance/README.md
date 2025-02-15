# Velo performance test

## Environment:

- CPU: Intel(R) Core(TM) i5-10400 CPU @ 2.90GHz
- Memory: Speed: 2667 MT/s, Type: DDR4, Size: 16GB
- SSD: Samsung SSD 970 EVO Plus 500GB, fio 4k random read 230K IOPS
- OS: Ubuntu 20.04.3 LTS
- Redis benchmark: redis-benchmark 6.2.5

TIPS: Use fio to test your disk performance, for example:

```shell
fio --name=randread --ioengine=libaio --iodepth=32 --rw=randread --bs=4k --direct=1 --size=1G --numjobs=1 --runtime=60
```

## Single thread

Configuration velo.properties (no compress):
```properties
slotNumber=1
netWorkers=1
estimateKeyNumber=10000000
estimateOneValueLength=200
kv.lru.maxSize=1000000
```

Run velo:
```shell
java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

Then run set test:
```bash
redis-benchmark -p 7379 -r 1000000 -n 1000000 -c 2 -d 200 -t set --threads 1
```

Benchmark result:
```code
99.990% <= 0.807 milliseconds (cumulative count 999897)
Summary:
  throughput summary: 66564.60 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.027     0.008     0.023     0.039     0.175     8.599
```

Then run get test:
```bash
redis-benchmark -p 7379 -r 1000000 -n 1000000 -c 2 -d 200 -t get --threads 1
```

Benchmark result:
```code
99.996% <= 0.407 milliseconds (cumulative count 999962)
Summary:
  throughput summary: 68054.98 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.027     0.008     0.031     0.039     0.071     3.167
```

Metrics:
```text
slot_kv_lru_hit_ratio{slot="0",} 0.19853
slot_kv_lru_hit_total{slot="0",} 198530.0
slot_kv_lru_miss_total{slot="0",} 801470.0
```

### test do compress for each value

Configuration velo.properties (compress):
```properties
slotNumber=1
netWorkers=1
estimateKeyNumber=10000000
estimateOneValueLength=200
kv.lru.maxSize=1000000
isValueSetUseCompression=true
isOnDynTrainDictForCompression=true
```

Run velo:
```shell
java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

Train dict use first 1000 keys, Velo will train a zstd dictionary, it will take 10-20 ms:
```bash
redis-benchmark -p 7379 -r 1000000 -n 1000 -c 2 -d 200 -t set --threads 1
```

Then run set test:
```bash
redis-benchmark -p 7379 -r 1000000 -n 1000000 -c 2 -d 200 -t set --threads 1
```

Benchmark result:
```code
99.993% <= 0.903 milliseconds (cumulative count 999928)
Summary:
  throughput summary: 52943.67 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.034     0.016     0.031     0.047     0.175    10.623
```

Metrics:
```text
net_compressed_cost_time_avg_us{name="net_worker_0",} 1.990138
net_compressed_cost_time_total_ms{name="net_worker_0",} 1990.138
net_compression_ratio{name="net_worker_0",} 0.1
net_raw_total_length{name="net_worker_0",} 2.0E8
net_compressed_count{name="net_worker_0",} 1000000.0
net_compressed_total_length{name="net_worker_0",} 2.0E7
net_raw_count{name="net_worker_0",} 1001.0
fd_write_bytes_total{slot="0",} 6.59456E7
fd_write_count_total{slot="0",} 4025.0
fd_write_index{slot="0",} 1.0872832E8
fd_write_time_avg_us{slot="0",} 13.808944099378882
fd_write_time_total_us{slot="0",} 55581.0
```

Conclusion:
Do value compress use a trained dict, set throughput and latency are about 20% worse compared with no compress.

Then run get test:
```bash
redis-benchmark -p 7379 -r 1000000 -n 1000000 -c 2 -d 200 -t get --threads 1
```

Benchmark result:
```code
99.990% <= 0.303 milliseconds (cumulative count 999903)
Summary:
  throughput summary: 63395.46 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.029     0.008     0.031     0.039     0.071     8.287
```

Metrics:
```text
net_decompressed_cost_time_avg_us{name="net_worker_0",} 0.047556678673651986
net_decompressed_cost_time_total_ms{name="net_worker_0",} 41.139
net_decompressed_count{name="net_worker_0",} 865052.0
slot_kv_lru_hit_ratio{slot="0",} 0.257542
slot_kv_lru_hit_total{slot="0",} 257542.0
slot_kv_lru_miss_total{slot="0",} 742458.0
fd_read_bytes_total{slot="0",} 2.4889344E9
fd_read_count_total{slot="0",} 607650.0
fd_read_time_avg_us{slot="0",} 0.8629474203900271
fd_read_time_total_us{slot="0",} 524370.0
```

Conclusion:
Do value compress use a trained dict, get throughput and latency are about 10% worse (cache hit is better) compared with no compress.

## Multi thread

todo

## Perf cpu flame graph

get:
https://github.com/segment11/velo/blob/main/doc/performance/arthas-profiler-get.html

set:
https://github.com/segment11/velo/blob/main/doc/performance/arthas-profiler-set.html