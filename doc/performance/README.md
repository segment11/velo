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

Configuration velo.properties:

```properties
slotNumber=4
slotWorkers=4
netWorkers=2
estimateKeyNumber=10000000
estimateOneValueLength=200
kv.lru.maxSize=1000000
```

cpu used: 4-6 v cores, qps ~= 20w

```
redis-benchmark -p 7379 -r 10000000 -n 10000000 -c 8 -d 200 -t get
redis-benchmark -p 7379 -r 10000000 -n 10000000 -c 8 -d 200 -t set
```

### Metrics

For one slot: 

```text
big_key_count{slot="0",} 0.0
big_string_files_count{slot="0",} 0.0
big_string_files_delay_to_delete_count{slot="0",} 0.0
big_string_files_delete_byte_length_avg{slot="0",} NaN
big_string_files_delete_file_cost_us_avg{slot="0",} NaN
big_string_files_delete_file_count{slot="0",} 0.0
big_string_files_disk_usage{slot="0",} 0.0
big_string_files_read_byte_length_avg{slot="0",} NaN
big_string_files_read_file_cost_us_avg{slot="0",} NaN
big_string_files_read_file_count{slot="0",} 0.0
big_string_files_write_byte_length_avg{slot="0",} NaN
big_string_files_write_file_cost_us_avg{slot="0",} NaN
big_string_files_write_file_count{slot="0",} 0.0
binlog_current_offset_from_the_beginning{slot="0",} 0.0
binlog_disk_usage{slot="0",} 0.0
binlog_segment_read_time_avg_us{slot="0",} 0.0
chunk_current_segment_index{slot="0",} 270246.0
chunk_cv_count_when_over_half_segment_count{slot="0",} 0.0
chunk_disk_usage{slot="0",} 1.120559104E9
chunk_max_once_segment_size{slot="0",} 13.0
chunk_max_segment_index{slot="0",} 1048575.0
chunk_persist_call_count_total{slot="0",} 20221.0
chunk_persist_cv_count_avg{slot="0",} 158.2209089560358
chunk_persist_cv_count_total{slot="0",} 3199385.0
chunk_segment_number_when_over_half_estimate_key_number{slot="0",} 0.0
chunk_update_pvm_batch_cost_time_avg_us{slot="0",} 219.23109638494634
chunk_update_pvm_batch_cost_time_total_us{slot="0",} 4433072.0
fd_c_0_read_bytes_total{slot="0",} 2.709323776E9
fd_c_0_read_count_total{slot="0",} 661456.0
fd_c_0_read_time_avg_us{slot="0",} 1.1549218693306886
fd_c_0_read_time_total_us{slot="0",} 763930.0
fd_c_0_write_bytes_total{slot="0",} 8.41342976E8
fd_c_0_write_count_total{slot="0",} 83687.0
fd_c_0_write_index{slot="0",} 1.106927616E9
fd_c_0_write_time_avg_us{slot="0",} 6.097004313692688
fd_c_0_write_time_total_us{slot="0",} 510240.0
fd_c_1_write_index{slot="0",} 0.0
fd_k_0_after_lru_read_decompress_time_avg_us{slot="0",} 2.55397244851898
fd_k_0_after_lru_read_decompress_time_total_us{slot="0",} 3802109.0
fd_k_0_after_read_compress_count_total{slot="0",} 259754.0
fd_k_0_after_read_compress_ratio{slot="0",} 0.07088255652613867
fd_k_0_after_read_compress_time_avg_us{slot="0",} 8.985809650669479
fd_k_0_after_read_compress_time_total_us{slot="0",} 2334100.0
fd_k_0_lru_hit_counter{slot="0",} 1488704.0
fd_k_0_lru_hit_ratio{slot="0",} 0.8280463731421569
fd_k_0_lru_miss_counter{slot="0",} 309147.0
fd_k_0_read_bytes_total{slot="0",} 4.184322048E9
fd_k_0_read_count_total{slot="0",} 331410.0
fd_k_0_read_time_avg_us{slot="0",} 3.53459762831538
fd_k_0_read_time_total_us{slot="0",} 1171401.0
fd_k_0_write_bytes_total{slot="0",} 2.650406912E9
fd_k_0_write_count_total{slot="0",} 20221.0
fd_k_0_write_index{slot="0",} 1.073741824E9
fd_k_0_write_time_avg_us{slot="0",} 18.9705751446516
fd_k_0_write_time_total_us{slot="0",} 383604.0
key_loader_bucket_count{slot="0",} 262144.0
key_loader_disk_usage{slot="0",} 1.074528256E9
persist_key_count{slot="0",} 2969655.0
segment_batch_count_total{slot="0",} 205406.0
segment_batch_kv_count_avg{slot="0",} 15.575908201318365
segment_batch_kv_count_total{slot="0",} 3199385.0
slot_avg_ttl_in_second{slot="0",} 0.0
slot_kv_lru_current_count_total{slot="0",} 0.0
slot_kv_lru_cv_encoded_length_avg{slot="0",} 230.9923339367413
slot_kv_lru_hit_ratio{slot="0",} 0.4718167463944068
slot_kv_lru_hit_total{slot="0",} 1533512.0
slot_kv_lru_miss_total{slot="0",} 1716716.0
slot_last_seq{slot="0",} 2.884674333442048E17
wal_disk_usage{slot="0",} 6.7108864E8
wal_group_first_delay_short_values_size{slot="0",} 63.0
wal_group_first_delay_values_size{slot="0",} 97.0
wal_group_first_need_persist_count_total{slot="0",} 3.0
wal_group_first_need_persist_kv_count_total{slot="0",} 480.0
wal_group_first_need_persist_offset_total{slot="0",} 180380.0
wal_key_count{slot="0",} 966666.0
```

For total:

```text
# HELP request_time Request time in seconds.
# TYPE request_time summary
request_time{command="set",quantile="0.99",} NaN
request_time{command="set",quantile="0.999",} NaN
request_time_count{command="set",} 2.1E7
request_time_sum{command="set",} 93.07358137002578
request_time{command="get",quantile="0.99",} NaN
request_time{command="get",quantile="0.999",} NaN
request_time_count{command="get",} 1.3E7
request_time_sum{command="get",} 53.66583789099953
request_time{command="config",quantile="0.99",} NaN
request_time{command="config",quantile="0.999",} NaN
request_time_count{command="config",} 14.0
request_time_sum{command="config",} 0.044853453999999994
request_time{command="manage",quantile="0.99",} NaN
request_time{command="manage",quantile="0.999",} NaN
request_time_count{command="manage",} 4.0
request_time_sum{command="manage",} 0.041105164
# HELP request_handler Net worker request handler metrics.
# TYPE request_handler gauge
request_sample_to_train_size{worker_id="0",} 0.0
request_sample_to_train_size{worker_id="1",} 0.0
request_sample_to_train_size{worker_id="2",} 0.0
request_sample_to_train_size{worker_id="3",} 0.0
# HELP jvm_gc_collection_seconds Time spent in a given JVM garbage collector in seconds.
# TYPE jvm_gc_collection_seconds summary
jvm_gc_collection_seconds_count{gc="ZGC Minor Cycles",} 388.0
jvm_gc_collection_seconds_sum{gc="ZGC Minor Cycles",} 56.647
jvm_gc_collection_seconds_count{gc="ZGC Minor Pauses",} 1167.0
jvm_gc_collection_seconds_sum{gc="ZGC Minor Pauses",} 0.007
jvm_gc_collection_seconds_count{gc="ZGC Major Cycles",} 25.0
jvm_gc_collection_seconds_sum{gc="ZGC Major Cycles",} 56.655
jvm_gc_collection_seconds_count{gc="ZGC Major Pauses",} 122.0
jvm_gc_collection_seconds_sum{gc="ZGC Major Pauses",} 0.0
# HELP compress_stats Net worker request handle compress stats.
# TYPE compress_stats gauge
# HELP jvm_memory_objects_pending_finalization The number of objects waiting in the finalizer queue.
# TYPE jvm_memory_objects_pending_finalization gauge
jvm_memory_objects_pending_finalization 0.0
# HELP jvm_memory_bytes_used Used bytes of a given JVM memory area.
# TYPE jvm_memory_bytes_used gauge
jvm_memory_bytes_used{area="heap",} 2.860515328E9
jvm_memory_bytes_used{area="nonheap",} 7.9163016E7
# HELP jvm_memory_bytes_committed Committed (bytes) of a given JVM memory area.
# TYPE jvm_memory_bytes_committed gauge
jvm_memory_bytes_committed{area="heap",} 4.294967296E9
jvm_memory_bytes_committed{area="nonheap",} 1.0911744E8
# HELP jvm_memory_bytes_max Max (bytes) of a given JVM memory area.
# TYPE jvm_memory_bytes_max gauge
jvm_memory_bytes_max{area="heap",} 4.294967296E9
jvm_memory_bytes_max{area="nonheap",} -1.0
# HELP jvm_memory_bytes_init Initial bytes of a given JVM memory area.
# TYPE jvm_memory_bytes_init gauge
jvm_memory_bytes_init{area="heap",} 4.294967296E9
jvm_memory_bytes_init{area="nonheap",} 7667712.0
# HELP jvm_memory_pool_bytes_used Used bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_used gauge
jvm_memory_pool_bytes_used{pool="CodeHeap 'non-nmethods'",} 2761088.0
jvm_memory_pool_bytes_used{pool="Metaspace",} 6.5833008E7
jvm_memory_pool_bytes_used{pool="CodeHeap 'profiled nmethods'",} 810752.0
jvm_memory_pool_bytes_used{pool="Compressed Class Space",} 7420760.0
jvm_memory_pool_bytes_used{pool="ZGC Old Generation",} 2.58998272E9
jvm_memory_pool_bytes_used{pool="ZGC Young Generation",} 2.70532608E8
jvm_memory_pool_bytes_used{pool="CodeHeap 'non-profiled nmethods'",} 2337408.0
# HELP jvm_memory_pool_bytes_committed Committed bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_committed gauge
jvm_memory_pool_bytes_committed{pool="CodeHeap 'non-nmethods'",} 4128768.0
jvm_memory_pool_bytes_committed{pool="Metaspace",} 6.6977792E7
jvm_memory_pool_bytes_committed{pool="CodeHeap 'profiled nmethods'",} 2.1823488E7
jvm_memory_pool_bytes_committed{pool="Compressed Class Space",} 7864320.0
jvm_memory_pool_bytes_committed{pool="ZGC Old Generation",} 2.58998272E9
jvm_memory_pool_bytes_committed{pool="ZGC Young Generation",} 1.704984576E9
jvm_memory_pool_bytes_committed{pool="CodeHeap 'non-profiled nmethods'",} 8323072.0
# HELP jvm_memory_pool_bytes_max Max bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_max gauge
jvm_memory_pool_bytes_max{pool="CodeHeap 'non-nmethods'",} 7606272.0
jvm_memory_pool_bytes_max{pool="Metaspace",} -1.0
jvm_memory_pool_bytes_max{pool="CodeHeap 'profiled nmethods'",} 1.22023936E8
jvm_memory_pool_bytes_max{pool="Compressed Class Space",} 1.073741824E9
jvm_memory_pool_bytes_max{pool="ZGC Old Generation",} 4.294967296E9
jvm_memory_pool_bytes_max{pool="ZGC Young Generation",} 4.294967296E9
jvm_memory_pool_bytes_max{pool="CodeHeap 'non-profiled nmethods'",} 1.22028032E8
# HELP jvm_memory_pool_bytes_init Initial bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_init gauge
jvm_memory_pool_bytes_init{pool="CodeHeap 'non-nmethods'",} 2555904.0
jvm_memory_pool_bytes_init{pool="Metaspace",} 0.0
jvm_memory_pool_bytes_init{pool="CodeHeap 'profiled nmethods'",} 2555904.0
jvm_memory_pool_bytes_init{pool="Compressed Class Space",} 0.0
jvm_memory_pool_bytes_init{pool="ZGC Old Generation",} 0.0
jvm_memory_pool_bytes_init{pool="ZGC Young Generation",} 4.294967296E9
jvm_memory_pool_bytes_init{pool="CodeHeap 'non-profiled nmethods'",} 2555904.0
# HELP jvm_memory_pool_collection_used_bytes Used bytes after last collection of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_used_bytes gauge
jvm_memory_pool_collection_used_bytes{pool="ZGC Old Generation",} 2.58998272E9
jvm_memory_pool_collection_used_bytes{pool="ZGC Young Generation",} 1.048576E7
# HELP jvm_memory_pool_collection_committed_bytes Committed after last collection bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_committed_bytes gauge
jvm_memory_pool_collection_committed_bytes{pool="ZGC Old Generation",} 2.58998272E9
jvm_memory_pool_collection_committed_bytes{pool="ZGC Young Generation",} 1.704984576E9
# HELP jvm_memory_pool_collection_max_bytes Max bytes after last collection of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_max_bytes gauge
jvm_memory_pool_collection_max_bytes{pool="ZGC Old Generation",} 4.294967296E9
jvm_memory_pool_collection_max_bytes{pool="ZGC Young Generation",} 4.294967296E9
# HELP jvm_memory_pool_collection_init_bytes Initial after last collection bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_init_bytes gauge
jvm_memory_pool_collection_init_bytes{pool="ZGC Old Generation",} 0.0
jvm_memory_pool_collection_init_bytes{pool="ZGC Young Generation",} 4.294967296E9
# HELP dict Dict compressed metrics.
# TYPE dict gauge
dict_compressed_ratio_1{name="self_",} 0.0
dict_compressed_count_1{name="self_",} 0.0
# HELP keys Key analysis metrics.
# TYPE keys gauge
key_analysis_add_value_length_avg 190.92023203246276
key_analysis_all_key_count 218919.0
key_analysis_add_count 220314.0
# HELP jvm_buffer_pool_used_bytes Used bytes of a given JVM buffer pool.
# TYPE jvm_buffer_pool_used_bytes gauge
jvm_buffer_pool_used_bytes{pool="mapped",} 0.0
jvm_buffer_pool_used_bytes{pool="direct",} 1.8349973E7
jvm_buffer_pool_used_bytes{pool="mapped - 'non-volatile memory'",} 0.0
# HELP jvm_buffer_pool_capacity_bytes Bytes capacity of a given JVM buffer pool.
# TYPE jvm_buffer_pool_capacity_bytes gauge
jvm_buffer_pool_capacity_bytes{pool="mapped",} 0.0
jvm_buffer_pool_capacity_bytes{pool="direct",} 1.8349971E7
jvm_buffer_pool_capacity_bytes{pool="mapped - 'non-volatile memory'",} 0.0
# HELP jvm_buffer_pool_used_buffers Used buffers of a given JVM buffer pool.
# TYPE jvm_buffer_pool_used_buffers gauge
jvm_buffer_pool_used_buffers{pool="mapped",} 0.0
jvm_buffer_pool_used_buffers{pool="direct",} 11.0
jvm_buffer_pool_used_buffers{pool="mapped - 'non-volatile memory'",} 0.0
# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 984.08
# HELP process_start_time_seconds Start time of the process since unix epoch in seconds.
# TYPE process_start_time_seconds gauge
process_start_time_seconds 1.7701295918E9
# HELP process_open_fds Number of open file descriptors.
# TYPE process_open_fds gauge
process_open_fds 542.0
# HELP process_max_fds Maximum number of open file descriptors.
# TYPE process_max_fds gauge
process_max_fds 1048576.0
# HELP process_virtual_memory_bytes Virtual memory size in bytes.
# TYPE process_virtual_memory_bytes gauge
process_virtual_memory_bytes 9.40511232E10
# HELP process_resident_memory_bytes Resident memory size in bytes.
# TYPE process_resident_memory_bytes gauge
process_resident_memory_bytes 5.5194624E9
# HELP global Global metrics.
# TYPE global gauge
global_key_bucket_once_scan_max_loop_count 1024.0
global_slot_number 4.0
lru_prepare_mb_all 2052.0
static_memory_prepare_mb_wal_cache_all_slots 0.0
global_connected_client_count 2.0
global_key_initial_split_number 1.0
lru_prepare_mb_chunk_segment_merged_cv_buffer_all_slots 0.0
static_memory_prepare_mb_wal_cache_init_all_slots 0.0
global_slot_workers 4.0
static_memory_prepare_mb_fd_read_write_buffer_all_slots 0.0
lru_prepare_mb_kv_write_in_wal_all_slots 256.0
static_memory_prepare_mb_meta_chunk_segment_flag_seq_all_slots 52.0
global_chunk_segment_length 4096.0
global_chunk_segment_number_per_fd 524288.0
global_wal_one_charge_bucket_number 32.0
global_subscribe_client_count 0.0
global_chunk_fd_per_chunk 2.0
global_net_in_bytes 5.592004998E9
lru_prepare_mb_kv_big_string_all_slots 12.0
global_dict_size 0.0
global_blocking_client_count 0.0
global_net_out_bytes 1.934072589E9
global_key_buckets_per_slot 262144.0
global_system_load_average 1.29931640625
global_up_time 1.770129592956E12
global_estimate_key_number 1.0E7
global_key_wal_once_scan_max_loop_count 1024.0
lru_prepare_mb_kv_read_group_by_wal_group_all_slots 760.0
global_net_workers 2.0
lru_prepare_mb_fd_key_bucket_all_slots 1024.0
lru_prepare_mb_fd_chunk_data_all_slots 0.0
# HELP request_time_created Request time in seconds.
# TYPE request_time_created gauge
request_time_created{command="set",} 1.7701296176E9
request_time_created{command="get",} 1.770129773718E9
request_time_created{command="config",} 1.770129617581E9
request_time_created{command="manage",} 1.770129732815E9
```

### Perf cpu flame graph

get:
https://github.com/segment11/velo/blob/main/doc/performance/arthas-profiler-get-20260203.html

set:
https://github.com/segment11/velo/blob/main/doc/performance/arthas-profiler-set-20260203.html