# velo support prometheus metrics and expose them on /?metrics endpoint.

## global metrics

http url is /?metrics

metrics include:

- uptime
- dict count
- some static configuration items value

refer to: OneSlot.initMetricsCollect()

## one slot metrics

http url is /?manage&slot&0&view-metrics

need change 0 to target slot index

metrics include:

- lru cache hit rate
- chunk sub-segment decompress time cost

refer to: OneSlot.collect()
