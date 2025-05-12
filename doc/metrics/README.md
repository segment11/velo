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

## one slot metrics

http url is /?manage&slot&0&view-metrics

need change 0 to target slot index

metrics include:

- lru cache hit rate
- chunk sub-segment decompress time cost

refer to: OneSlot.collect()

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
