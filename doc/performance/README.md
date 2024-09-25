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

### configuration

- slotNumber=1
- netWorkers=1
- estimateKeyNumber=10000000
- estimateOneValueLength=200
- kv.lru.maxSize=1000000

### run command

```shell
java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

### test set

- redis-benchmark -p 7379 -c 1 -r 10000000 -n 10000000 -d 200 -t set -P 2
- qps ~= 35000
- 99.9% latency ~= 1.5ms, 99.99% latency ~= 8ms

TIPS: When first run 'set' test, Velo will train a zstd dictionary, it will take 10-20 ms, so run the test again will get a better result.

### test get

- redis-benchmark -p 7379 -c 1 -r 1000000 -n 1000000 -d 200 -t get
- when cache at the beginning hit 0%, qps ~= 6000
- when cache finally hit 100%, qps ~= 50000
- when cache hit 50%, qps ~= 20000-30000
- 99.9% latency < 1ms, 99.99% latency ~= 2ms