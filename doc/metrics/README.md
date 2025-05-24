# Velo support prometheus metrics and expose them on /?metrics endpoint.

## global metrics

http url is /?metrics

metrics include:

- uptime
- some static configuration items value
- request time cost by command
- dict count
- dict compression rate
- jvm metrics

refer to: OneSlot.initMetricsCollect()

example:
```text
# HELP dict Dict compressed metrics.
# TYPE dict gauge
dict_compressed_count_496335027{name="key:",} 1.2113019E7
dict_compressed_ratio_1{name="self_",} 0.0
dict_compressed_count_1{name="self_",} 0.0
dict_compressed_ratio_496335027{name="key:",} 0.1
# HELP request_time Request time in seconds.
# TYPE request_time summary
request_time{command="set",quantile="0.99",} 1.6802E-4
request_time{command="set",quantile="0.999",} 0.018245039
request_time_count{command="set",} 1.2120817E7
request_time_sum{command="set",} 191.40645069800007
request_time{command="get",quantile="0.99",} NaN
request_time{command="get",quantile="0.999",} NaN
request_time_count{command="get",} 1.0
request_time_sum{command="get",} 0.003322324
request_time{command="config",quantile="0.99",} 6.61974E-4
request_time{command="config",quantile="0.999",} 6.61974E-4
request_time_count{command="config",} 6.0
request_time_sum{command="config",} 0.062310497
request_time{command="manage",quantile="0.99",} 0.00210538
request_time{command="manage",quantile="0.999",} 0.00210538
request_time_count{command="manage",} 3.0
request_time_sum{command="manage",} 0.021832734
# HELP jvm_memory_objects_pending_finalization The number of objects waiting in the finalizer queue.
# TYPE jvm_memory_objects_pending_finalization gauge
jvm_memory_objects_pending_finalization 0.0
# HELP jvm_memory_bytes_used Used bytes of a given JVM memory area.
# TYPE jvm_memory_bytes_used gauge
jvm_memory_bytes_used{area="heap",} 4.8234496E9
jvm_memory_bytes_used{area="nonheap",} 8.9959632E7
# HELP jvm_memory_bytes_committed Committed (bytes) of a given JVM memory area.
# TYPE jvm_memory_bytes_committed gauge
jvm_memory_bytes_committed{area="heap",} 8.589934592E9
jvm_memory_bytes_committed{area="nonheap",} 1.00728832E8
# HELP jvm_memory_bytes_max Max (bytes) of a given JVM memory area.
# TYPE jvm_memory_bytes_max gauge
jvm_memory_bytes_max{area="heap",} 8.589934592E9
jvm_memory_bytes_max{area="nonheap",} -1.0
# HELP jvm_memory_bytes_init Initial bytes of a given JVM memory area.
# TYPE jvm_memory_bytes_init gauge
jvm_memory_bytes_init{area="heap",} 8.589934592E9
jvm_memory_bytes_init{area="nonheap",} 7667712.0
# HELP jvm_memory_pool_bytes_used Used bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_used gauge
jvm_memory_pool_bytes_used{pool="CodeHeap 'non-nmethods'",} 2173056.0
jvm_memory_pool_bytes_used{pool="Metaspace",} 6.327048E7
jvm_memory_pool_bytes_used{pool="CodeHeap 'profiled nmethods'",} 1.1768576E7
jvm_memory_pool_bytes_used{pool="Compressed Class Space",} 7076352.0
jvm_memory_pool_bytes_used{pool="ZGC Old Generation",} 3.099590656E9
jvm_memory_pool_bytes_used{pool="ZGC Young Generation",} 1.723858944E9
jvm_memory_pool_bytes_used{pool="CodeHeap 'non-profiled nmethods'",} 5671168.0
# HELP jvm_memory_pool_bytes_committed Committed bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_committed gauge
jvm_memory_pool_bytes_committed{pool="CodeHeap 'non-nmethods'",} 2555904.0
jvm_memory_pool_bytes_committed{pool="Metaspace",} 6.4159744E7
jvm_memory_pool_bytes_committed{pool="CodeHeap 'profiled nmethods'",} 1.8743296E7
jvm_memory_pool_bytes_committed{pool="Compressed Class Space",} 7471104.0
jvm_memory_pool_bytes_committed{pool="ZGC Old Generation",} 3.099590656E9
jvm_memory_pool_bytes_committed{pool="ZGC Young Generation",} 5.490343936E9
jvm_memory_pool_bytes_committed{pool="CodeHeap 'non-profiled nmethods'",} 7798784.0
# HELP jvm_memory_pool_bytes_max Max bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_max gauge
jvm_memory_pool_bytes_max{pool="CodeHeap 'non-nmethods'",} 5840896.0
jvm_memory_pool_bytes_max{pool="Metaspace",} -1.0
jvm_memory_pool_bytes_max{pool="CodeHeap 'profiled nmethods'",} 1.22908672E8
jvm_memory_pool_bytes_max{pool="Compressed Class Space",} 1.073741824E9
jvm_memory_pool_bytes_max{pool="ZGC Old Generation",} 8.589934592E9
jvm_memory_pool_bytes_max{pool="ZGC Young Generation",} 8.589934592E9
jvm_memory_pool_bytes_max{pool="CodeHeap 'non-profiled nmethods'",} 1.22908672E8
# HELP jvm_memory_pool_bytes_init Initial bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_bytes_init gauge
jvm_memory_pool_bytes_init{pool="CodeHeap 'non-nmethods'",} 2555904.0
jvm_memory_pool_bytes_init{pool="Metaspace",} 0.0
jvm_memory_pool_bytes_init{pool="CodeHeap 'profiled nmethods'",} 2555904.0
jvm_memory_pool_bytes_init{pool="Compressed Class Space",} 0.0
jvm_memory_pool_bytes_init{pool="ZGC Old Generation",} 0.0
jvm_memory_pool_bytes_init{pool="ZGC Young Generation",} 8.589934592E9
jvm_memory_pool_bytes_init{pool="CodeHeap 'non-profiled nmethods'",} 2555904.0
# HELP jvm_memory_pool_collection_used_bytes Used bytes after last collection of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_used_bytes gauge
jvm_memory_pool_collection_used_bytes{pool="ZGC Old Generation",} 3.099590656E9
jvm_memory_pool_collection_used_bytes{pool="ZGC Young Generation",} 9.68884224E8
# HELP jvm_memory_pool_collection_committed_bytes Committed after last collection bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_committed_bytes gauge
jvm_memory_pool_collection_committed_bytes{pool="ZGC Old Generation",} 3.099590656E9
jvm_memory_pool_collection_committed_bytes{pool="ZGC Young Generation",} 5.490343936E9
# HELP jvm_memory_pool_collection_max_bytes Max bytes after last collection of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_max_bytes gauge
jvm_memory_pool_collection_max_bytes{pool="ZGC Old Generation",} 8.589934592E9
jvm_memory_pool_collection_max_bytes{pool="ZGC Young Generation",} 8.589934592E9
# HELP jvm_memory_pool_collection_init_bytes Initial after last collection bytes of a given JVM memory pool.
# TYPE jvm_memory_pool_collection_init_bytes gauge
jvm_memory_pool_collection_init_bytes{pool="ZGC Old Generation",} 0.0
jvm_memory_pool_collection_init_bytes{pool="ZGC Young Generation",} 8.589934592E9
# HELP jvm_buffer_pool_used_bytes Used bytes of a given JVM buffer pool.
# TYPE jvm_buffer_pool_used_bytes gauge
jvm_buffer_pool_used_bytes{pool="mapped",} 0.0
jvm_buffer_pool_used_bytes{pool="direct",} 4775827.0
jvm_buffer_pool_used_bytes{pool="mapped - 'non-volatile memory'",} 0.0
# HELP jvm_buffer_pool_capacity_bytes Bytes capacity of a given JVM buffer pool.
# TYPE jvm_buffer_pool_capacity_bytes gauge
jvm_buffer_pool_capacity_bytes{pool="mapped",} 0.0
jvm_buffer_pool_capacity_bytes{pool="direct",} 4775825.0
jvm_buffer_pool_capacity_bytes{pool="mapped - 'non-volatile memory'",} 0.0
# HELP jvm_buffer_pool_used_buffers Used buffers of a given JVM buffer pool.
# TYPE jvm_buffer_pool_used_buffers gauge
jvm_buffer_pool_used_buffers{pool="mapped",} 0.0
jvm_buffer_pool_used_buffers{pool="direct",} 12.0
jvm_buffer_pool_used_buffers{pool="mapped - 'non-volatile memory'",} 0.0
# HELP jvm_gc_collection_seconds Time spent in a given JVM garbage collector in seconds.
# TYPE jvm_gc_collection_seconds summary
jvm_gc_collection_seconds_count{gc="ZGC Minor Cycles",} 75.0
jvm_gc_collection_seconds_sum{gc="ZGC Minor Cycles",} 36.358
jvm_gc_collection_seconds_count{gc="ZGC Minor Pauses",} 230.0
jvm_gc_collection_seconds_sum{gc="ZGC Minor Pauses",} 0.001
jvm_gc_collection_seconds_count{gc="ZGC Major Cycles",} 9.0
jvm_gc_collection_seconds_sum{gc="ZGC Major Cycles",} 39.069
jvm_gc_collection_seconds_count{gc="ZGC Major Pauses",} 40.0
jvm_gc_collection_seconds_sum{gc="ZGC Major Pauses",} 0.0
# HELP keys Key analysis metrics.
# TYPE keys gauge
key_analysis_add_value_length_avg 0.37
key_analysis_all_key_count 800000.0
key_analysis_add_count 800000.0
# HELP global Global metrics.
# TYPE global gauge
global_slot_number 8.0
lru_prepare_mb_all 7688.0
static_memory_prepare_mb_wal_cache_all_slots 0.0
global_connected_client_count 18.0
global_key_initial_split_number 1.0
lru_prepare_mb_chunk_segment_merged_cv_buffer_all_slots 0.0
static_memory_prepare_mb_wal_cache_init_all_slots 0.0
global_slot_workers 8.0
static_memory_prepare_mb_fd_read_write_buffer_all_slots 64.0
lru_prepare_mb_kv_write_in_wal_all_slots 4096.0
static_memory_prepare_mb_meta_chunk_segment_flag_seq_all_slots 104.0
global_chunk_segment_length 4096.0
global_chunk_segment_number_per_fd 524288.0
global_wal_one_charge_bucket_number 32.0
global_subscribe_client_count 0.0
global_key_once_scan_max_read_count 100.0
global_chunk_fd_per_chunk 2.0
global_net_in_bytes 2.957489533E9
lru_prepare_mb_kv_big_string_all_slots 24.0
global_dict_size 1.0
global_blocking_client_count 0.0
global_net_out_bytes 6.061223E7
global_key_buckets_per_slot 262144.0
global_up_time 1.748058576442E12
global_estimate_key_number 1.0E7
lru_prepare_mb_kv_read_group_by_wal_group_all_slots 1520.0
global_net_workers 4.0
lru_prepare_mb_fd_key_bucket_all_slots 2048.0
lru_prepare_mb_fd_chunk_data_all_slots 0.0
# HELP compress_stats Net worker request handle compress stats.
# TYPE compress_stats gauge
slot_raw_count{name="slot_worker_0",} 913.0
slot_compressed_cost_time_total_ms{name="slot_worker_0",} 4550.303
slot_raw_total_length{name="slot_worker_0",} 3.030816E8
slot_compressed_cost_time_avg_us{name="slot_worker_0",} 3.0045018306432176
slot_compressed_total_length{name="slot_worker_0",} 3.04725E7
slot_compressed_count{name="slot_worker_0",} 1514495.0
slot_compression_ratio{name="slot_worker_0",} 0.10054223021126983
slot_raw_count{name="slot_worker_1",} 964.0
slot_compressed_cost_time_total_ms{name="slot_worker_1",} 4556.848
slot_raw_total_length{name="slot_worker_1",} 3.032878E8
slot_compressed_cost_time_avg_us{name="slot_worker_1",} 3.0068777116085714
slot_compressed_total_length{name="slot_worker_1",} 3.05023E7
slot_compressed_count{name="slot_worker_1",} 1515475.0
slot_compression_ratio{name="slot_worker_1",} 0.10057212983839113
slot_raw_count{name="slot_worker_2",} 963.0
slot_compressed_cost_time_total_ms{name="slot_worker_2",} 4578.118
slot_raw_total_length{name="slot_worker_2",} 3.030976E8
slot_compressed_cost_time_avg_us{name="slot_worker_2",} 3.022807811029861
slot_compressed_total_length{name="slot_worker_2",} 3.04831E7
slot_compressed_count{name="slot_worker_2",} 1514525.0
slot_compression_ratio{name="slot_worker_2",} 0.10057189499355983
slot_raw_count{name="slot_worker_3",} 994.0
slot_compressed_cost_time_total_ms{name="slot_worker_3",} 4549.158
slot_raw_total_length{name="slot_worker_3",} 3.028988E8
slot_compressed_cost_time_avg_us{name="slot_worker_3",} 3.005720515361744
slot_compressed_total_length{name="slot_worker_3",} 3.04688E7
slot_compressed_count{name="slot_worker_3",} 1513500.0
slot_compression_ratio{name="slot_worker_3",} 0.1005906923368465
slot_raw_count{name="slot_worker_4",} 946.0
slot_compressed_cost_time_total_ms{name="slot_worker_4",} 4579.785
slot_raw_total_length{name="slot_worker_4",} 3.026676E8
slot_compressed_cost_time_avg_us{name="slot_worker_4",} 3.028173251379272
slot_compressed_total_length{name="slot_worker_4",} 3.043704E7
slot_compressed_count{name="slot_worker_4",} 1512392.0
slot_compression_ratio{name="slot_worker_4",} 0.10056259738406093
slot_raw_count{name="slot_worker_5",} 942.0
slot_compressed_cost_time_total_ms{name="slot_worker_5",} 4591.714
slot_raw_total_length{name="slot_worker_5",} 3.032236E8
slot_compressed_cost_time_avg_us{name="slot_worker_5",} 3.030482267406559
slot_compressed_total_length{name="slot_worker_5",} 3.049192E7
slot_compressed_count{name="slot_worker_5",} 1515176.0
slot_compression_ratio{name="slot_worker_5",} 0.1005591913030516
slot_raw_count{name="slot_worker_6",} 1001.0
slot_compressed_cost_time_total_ms{name="slot_worker_6",} 4577.499
slot_raw_total_length{name="slot_worker_6",} 3.02748E8
slot_compressed_cost_time_avg_us{name="slot_worker_6",} 3.02596746695894
slot_compressed_total_length{name="slot_worker_6",} 3.045498E7
slot_compressed_count{name="slot_worker_6",} 1512739.0
slot_compression_ratio{name="slot_worker_6",} 0.10059514844028697
slot_raw_count{name="slot_worker_7",} 984.0
slot_compressed_cost_time_total_ms{name="slot_worker_7",} 4565.752
slot_raw_total_length{name="slot_worker_7",} 3.031584E8
slot_compressed_cost_time_avg_us{name="slot_worker_7",} 3.0140796721432683
slot_compressed_total_length{name="slot_worker_7",} 3.049296E7
slot_compressed_count{name="slot_worker_7",} 1514808.0
slot_compression_ratio{name="slot_worker_7",} 0.10058424902625163
# HELP request_handler Net worker request handler metrics.
# TYPE request_handler gauge
request_sample_to_train_size{worker_id="0",} 913.0
request_sample_to_train_size{worker_id="1",} 964.0
request_sample_to_train_size{worker_id="2",} 963.0
request_sample_to_train_size{worker_id="3",} 994.0
request_sample_to_train_size{worker_id="4",} 946.0
request_sample_to_train_size{worker_id="5",} 942.0
request_sample_to_train_size{worker_id="6",} 0.0
request_sample_to_train_size{worker_id="7",} 984.0
# HELP request_time_created Request time in seconds.
# TYPE request_time_created gauge
request_time_created{command="set",} 1.748058611497E9
request_time_created{command="get",} 1.748058596765E9
request_time_created{command="config",} 1.748058611468E9
request_time_created{command="manage",} 1.748058678285E9
```

## one slot metrics

http url is /?manage&slot&0&view-metrics

need change 0 to target slot index

metrics include:

- lru cache hit rate
- chunk sub-segment decompress time cost

refer to: OneSlot.collect()

example:
```text
big_key_count{slot="7",} 0.0
big_string_files_count{slot="7",} 0.0
binlog_current_offset_from_the_beginning{slot="7",} 0.0
chunk_current_segment_index{slot="7",} 991089.0
chunk_cv_count_when_over_half_segment_count{slot="7",} 0.0
chunk_max_once_segment_size{slot="7",} 0.0
chunk_max_segment_index{slot="7",} 1048575.0
chunk_segment_number_when_over_half_estimate_key_number{slot="7",} 0.0
fd_c_0_read_bytes_total{slot="7",} 3.03734784E8
fd_c_0_read_count_total{slot="7",} 74154.0
fd_c_0_read_time_avg_us{slot="7",} 70.35052728106373
fd_c_0_read_time_total_us{slot="7",} 5216773.0
fd_c_0_write_index{slot="7",} 2.147454976E9
fd_c_1_read_bytes_total{slot="7",} 1.2260102144E10
fd_c_1_read_count_total{slot="7",} 2993189.0
fd_c_1_read_time_avg_us{slot="7",} 13.096710231128071
fd_c_1_read_time_total_us{slot="7",} 3.9200929E7
fd_c_1_write_index{slot="7",} 1.912016896E9
fd_k_0_after_lru_read_decompress_time_avg_us{slot="7",} 7.710309768151976
fd_k_0_after_lru_read_decompress_time_total_us{slot="7",} 2.4553813E7
fd_k_0_lru_hit_counter{slot="7",} 3184543.0
fd_k_0_lru_hit_ratio{slot="7",} 1.0
fd_k_0_read_bytes_total{slot="7",} 1.073741824E9
fd_k_0_read_count_total{slot="7",} 8192.0
fd_k_0_read_time_avg_us{slot="7",} 225.750244140625
fd_k_0_read_time_total_us{slot="7",} 1849346.0
fd_k_0_write_index{slot="7",} 1.073741824E9
key_loader_bucket_count{slot="7",} 262144.0
persist_key_count{slot="7",} 4872565.0
slot_avg_ttl_in_second{slot="7",} 0.0
slot_kv_lru_current_count_total{slot="7",} 999424.0
slot_kv_lru_cv_encoded_length_avg{slot="7",} 116.80563221306934
slot_kv_lru_hit_ratio{slot="7",} 0.4212242469249158
slot_kv_lru_hit_total{slot="7",} 2317663.0
slot_kv_lru_miss_total{slot="7",} 3184544.0
slot_last_seq{slot="7",} 0.0
slot_pending_submit_index_job_count{slot="7",} 0.0
wal_group_first_delay_short_values_size{slot="7",} 0.0
wal_group_first_delay_values_size{slot="7",} 65.0
wal_group_first_need_persist_count_total{slot="7",} 0.0
wal_group_first_need_persist_kv_count_total{slot="7",} 0.0
wal_group_first_need_persist_offset_total{slot="7",} 0.0
wal_key_count{slot="7",} 693946.0
```

## prometheus config

```
scrape_configs:
  - job_name: velo
    metrics_path: /?metrics
    static_configs:
      - targets: ['127.0.0.1:7379']
  
  - job_name: velo-slot0
    metrics_path: /?manage&slot&0&view-metrics
    static_configs:
      - targets: ['127.0.0.1:7379']
  
  - job_name: velo-slot1
    metrics_path: /?manage&slot&1&view-metrics
    static_configs:
      - targets: ['127.0.0.1:7379']
```

## grafana dashboard

import file "Velo-Overview-20250512.json"
