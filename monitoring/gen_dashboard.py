#!/usr/bin/env python3
"""
Generate the Velo Grafana dashboards (one for a singleton or a whole cluster).

Three dashboards keep each view focused:
  1. velo-global.json        Global registry metrics ($instance).
                             JVM, request_time, global_*, compress_stats, dict.
  2. velo-slot-storage.json  Per-slot storage ($instance,$slot): WAL, key
                             buckets, chunks, disk usage, cache/LRU.
  3. velo-slot-io.json       Per-slot file I/O, big strings, replication.

Global metrics come from the `velo-global` job. Per-slot metrics come from the
`velo-slot0` / `velo-slot-last` jobs and are filtered by $slot (values 0, last).
"""
import json

DS = {"type": "prometheus", "uid": "velo-prom"}
RATE = "$__rate_interval"
_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


class Grid:
    """Left-to-right, wrapping panel packer on the 24-column Grafana grid."""

    def __init__(self):
        self.y = 0
        self.x = 0
        self.row_h = 0
        self.panels = []

    def row(self, title):
        if self.x:
            self.y += self.row_h
            self.x = 0
            self.row_h = 0
        self.panels.append({
            "id": nid(), "type": "row", "title": title, "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": self.y},
        })
        self.y += 1

    def add(self, panel, w, h):
        if self.x + w > 24:
            self.y += self.row_h
            self.x = 0
            self.row_h = 0
        panel["gridPos"] = {"h": h, "w": w, "x": self.x, "y": self.y}
        self.x += w
        self.row_h = max(self.row_h, h)
        self.panels.append(panel)


def target(expr, legend=None):
    t = {"datasource": DS, "expr": expr, "refId": chr(65 + target.n % 26)}
    target.n += 1
    if legend is not None:
        t["legendFormat"] = legend
    return t
target.n = 0


def ts(title, targets, unit="short", desc=None, stack=False, minv=None, decimals=None):
    custom = {
        "drawStyle": "line", "lineInterpolation": "smooth", "lineWidth": 1,
        "fillOpacity": 12, "gradientMode": "opacity", "showPoints": "never",
        "spanNulls": False, "axisPlacement": "auto",
        "stacking": {"mode": "normal" if stack else "none", "group": "A"},
    }
    if stack:
        custom["fillOpacity"] = 30
    fc = {"unit": unit}
    if minv is not None:
        fc["min"] = minv
    if decimals is not None:
        fc["decimals"] = decimals
    p = {
        "id": nid(), "type": "timeseries", "title": title, "datasource": DS,
        "targets": targets,
        "fieldConfig": {"defaults": {"custom": custom, **fc}, "overrides": []},
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom",
                       "calcs": ["lastNotNull", "max", "mean"], "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }
    if desc:
        p["description"] = desc
    return p


def stat(title, expr, unit="short", legend="{{instance}}", desc=None,
         graph=True, color="thresholds", thresholds=None, decimals=None):
    p = {
        "id": nid(), "type": "stat", "title": title, "datasource": DS,
        "targets": [target(expr, legend)],
        "fieldConfig": {"defaults": {
            "unit": unit, "color": {"mode": color},
            "thresholds": {"mode": "absolute",
                           "steps": thresholds or [{"color": "green", "value": None}]},
        }, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "orientation": "auto", "textMode": "auto", "colorMode": "value",
            "graphMode": "area" if graph else "none", "justifyMode": "auto",
        },
    }
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    if desc:
        p["description"] = desc
    return p


I = 'instance=~"$instance"'
S = 'instance=~"$instance",slot=~"$slot"'


# ---- dashboard 1: global -------------------------------------------------
# rows: Overview (health headline) / Commands & Traffic / Compression & Dict /
#       Runtime (JVM & OS). Static config/tuning metrics are intentionally omitted.
def build_global(g):
    g.row("Overview")
    g.add(stat("Uptime", f'(time()*1000 - global_up_time{{{I}}})/3600000',
               unit="h", desc="Hours since process start", graph=False, decimals=1), 6, 4)
    g.add(stat("Total QPS", f'sum by(instance)(rate(request_time_count{{{I}}}[{RATE}]))',
               unit="reqps", color="value"), 6, 4)
    g.add(stat("p99 Latency", f'max by(instance)(request_time{{{I},quantile="0.99"}})*1000',
               unit="ms", decimals=3, thresholds=[
                   {"color": "green", "value": None},
                   {"color": "yellow", "value": 1}, {"color": "red", "value": 10}]), 6, 4)
    g.add(stat("Connected Clients", f'sum by(instance)(global_connected_client_count{{{I}}})',
               color="value", graph=False), 6, 4)
    g.add(stat("Heap Used", f'sum by(instance)(jvm_memory_bytes_used{{{I},area="heap"}})',
               unit="bytes", color="value"), 6, 4)
    g.add(stat("CPU (cores)", f'sum by(instance)(rate(process_cpu_seconds_total{{{I}}}[{RATE}]))',
               unit="short", decimals=2, color="value"), 6, 4)
    g.add(stat("Compression Ratio", f'avg by(instance)(slot_compression_ratio{{{I}}})',
               unit="percentunit", decimals=3, graph=False, color="value",
               desc="compressed / raw bytes; lower is better", thresholds=[
                   {"color": "green", "value": None}, {"color": "yellow", "value": 0.5}]), 6, 4)
    g.add(stat("Total Keys", f'sum by(instance)(key_analysis_all_key_count{{{I}}})',
               unit="short", graph=False), 6, 4)
    g.add(stat("Error Reply QPS", f'sum by(instance)(rate(request_error_reply_count_total{{{I}}}[{RATE}]))',
               unit="reqps", decimals=3, color="value", desc="error replies per second", thresholds=[
                   {"color": "green", "value": None},
                   {"color": "yellow", "value": 1}, {"color": "red", "value": 10}]), 6, 4)

    g.row("Commands & Traffic")
    g.add(ts("Commands QPS (by command)",
             [target(f'sum by(command)(rate(request_time_count{{{I}}}[{RATE}]))', "{{command}}")],
             unit="reqps"), 12, 8)
    g.add(ts("Command Latency p99 (ms)",
             [target(f'request_time{{{I},quantile="0.99"}}*1000', "{{command}} {{instance}}")],
             unit="ms", decimals=3), 12, 8)
    g.add(ts("Command Latency p999 (ms)",
             [target(f'request_time{{{I},quantile="0.999"}}*1000', "{{command}} {{instance}}")],
             unit="ms", decimals=3), 12, 8)
    g.add(ts("Command Avg Latency (ms)",
             [target(f'rate(request_time_sum{{{I}}}[{RATE}])/rate(request_time_count{{{I}}}[{RATE}])*1000',
                     "{{command}} {{instance}}")], unit="ms", decimals=3), 12, 8)
    g.add(ts("Net In / Out (bytes/s)",
             [target(f'rate(global_net_in_bytes{{{I}}}[{RATE}])', "in {{instance}}"),
              target(f'rate(global_net_out_bytes{{{I}}}[{RATE}])', "out {{instance}}")],
             unit="Bps"), 12, 8)
    g.add(ts("Clients (connected / subscribe / blocking)",
             [target(f'global_connected_client_count{{{I}}}', "connected {{instance}}"),
              target(f'global_subscribe_client_count{{{I}}}', "subscribe {{instance}}"),
              target(f'global_blocking_client_count{{{I}}}', "blocking {{instance}}")],
             unit="short"), 12, 8)
    g.add(ts("Error Reply Rate (per worker)",
             [target(f'rate(request_error_reply_count_total{{{I}}}[{RATE}])', "worker {{worker_id}} {{instance}}")],
             unit="reqps", desc="error replies per second per net worker", minv=0), 12, 8)

    g.row("Compression & Dict")
    g.add(ts("Compression Ratio (per slot worker)",
             [target(f'slot_compression_ratio{{{I}}}', "{{name}} {{instance}}")],
             unit="percentunit", desc="compressed / raw bytes; lower is better", minv=0), 8, 7)
    g.add(ts("Compressed Count Rate",
             [target(f'rate(slot_compressed_count{{{I}}}[{RATE}])', "{{name}} {{instance}}")],
             unit="ops"), 8, 7)
    g.add(ts("Compress Cost Avg (µs)",
             [target(f'slot_compressed_cost_time_avg_us{{{I}}}', "{{name}} {{instance}}")],
             unit="µs"), 8, 7)
    g.add(ts("Raw vs Compressed Length",
             [target(f'slot_total_input_length{{{I}}}', "raw {{name}}"),
              target(f'slot_compressed_total_length{{{I}}}', "compressed {{name}}")],
             unit="bytes"), 8, 7)
    g.add(ts("Global Dict Size",
             [target(f'global_dict_size{{{I}}}', "{{instance}}")], unit="short"), 8, 7)
    g.add(ts("Dict Compression Ratio",
             [target(f'{{__name__=~"dict_compressed_ratio_.+",{I}}}', "{{name}} {{instance}}")],
             unit="percentunit", minv=0), 8, 7)

    g.row("Runtime (JVM & OS)")
    g.add(ts("CPU Usage (cores)",
             [target(f'rate(process_cpu_seconds_total{{{I}}}[{RATE}])', "{{instance}}")],
             unit="short", decimals=2), 8, 7)
    g.add(ts("Heap Memory",
             [target(f'jvm_memory_bytes_used{{{I},area="heap"}}', "used {{instance}}"),
              target(f'jvm_memory_bytes_committed{{{I},area="heap"}}', "committed {{instance}}"),
              target(f'jvm_memory_bytes_max{{{I},area="heap"}}', "max {{instance}}")],
             unit="bytes"), 8, 7)
    g.add(ts("Resident Memory (RSS)",
             [target(f'process_resident_memory_bytes{{{I}}}', "{{instance}}")], unit="bytes"), 8, 7)
    g.add(ts("GC Time Rate (s/s)",
             [target(f'rate(jvm_gc_collection_seconds_sum{{{I}}}[{RATE}])', "{{gc}} {{instance}}")],
             unit="s"), 8, 7)
    g.add(ts("GC Count Rate (/s)",
             [target(f'rate(jvm_gc_collection_seconds_count{{{I}}}[{RATE}])', "{{gc}} {{instance}}")],
             unit="ops"), 8, 7)
    g.add(ts("System Load / Open FDs",
             [target(f'global_system_load_average{{{I}}}', "load {{instance}}"),
              target(f'process_open_fds{{{I}}}', "open-fds {{instance}}")], unit="short"), 8, 7)


# ---- dashboard 2: per-slot capacity & persistence -----------------------
# rows: Capacity (will I run out) / Persist I/O & Latency (is storage fast).
def build_slot_capacity(g):
    g.row("Capacity — will I run out? (slot 0 & last)")
    g.add(ts("Persisted Key Count",
             [target(f'persist_key_count{{{S}}}', "s{{slot}} {{instance}}")], unit="short"), 8, 7)
    g.add(ts("WAL Key Count",
             [target(f'wal_key_count{{{S}}}', "s{{slot}} {{instance}}")], unit="short"), 8, 7)
    g.add(ts("Disk Usage Breakdown (stacked)",
             [target(f'wal_disk_usage{{{S}}}', "wal s{{slot}}"),
              target(f'chunk_disk_usage{{{S}}}', "chunk s{{slot}}"),
              target(f'key_loader_disk_usage{{{S}}}', "keyloader s{{slot}}"),
              target(f'binlog_disk_usage{{{S}}}', "binlog s{{slot}}"),
              target(f'big_string_files_disk_usage{{{S}}}', "bigstring s{{slot}}")],
             unit="bytes", stack=True), 8, 7)
    g.add(ts("Chunk Segment Fill Rate",
             [target(f'chunk_segment_fill_rate{{{S}}}', "s{{slot}} {{instance}}")],
             unit="percentunit", minv=0,
             desc="used chunk segments / total; capacity headroom"), 8, 7)
    g.add(ts("Chunk Segments Used / Reusable",
             [target(f'chunk_segment_used_count{{{S}}}', "used s{{slot}}"),
              target(f'chunk_segment_reusable_count{{{S}}}', "reusable s{{slot}}")], unit="short"), 8, 7)
    g.add(ts("Chunk Segment Cursor / Max Index",
             [target(f'chunk_current_segment_index{{{S}}}', "cursor s{{slot}}"),
              target(f'chunk_max_segment_index{{{S}}}', "max s{{slot}}")], unit="short"), 8, 7)
    g.add(ts("Key Loader Bucket Count",
             [target(f'key_loader_bucket_count{{{S}}}', "s{{slot}} {{instance}}")], unit="short"), 8, 7)
    g.add(ts("Avg TTL (s)",
             [target(f'slot_avg_ttl_in_second{{{S}}}', "s{{slot}} {{instance}}")], unit="s"), 8, 7)

    g.row("Persist I/O & Latency — is storage fast? (slot 0 & last)")
    g.add(ts("Key Bucket Read/Write Avg (µs)",
             [target(f'{{__name__=~"fd_k_\\\\d+_read_time_avg_us",{S}}}', "read {{__name__}} s{{slot}}"),
              target(f'{{__name__=~"fd_k_\\\\d+_write_time_avg_us",{S}}}', "write {{__name__}} s{{slot}}")],
             unit="µs"), 8, 7)
    g.add(ts("Chunk Data Read/Write Avg (µs)",
             [target(f'{{__name__=~"fd_c_\\\\d+_read_time_avg_us",{S}}}', "read {{__name__}} s{{slot}}"),
              target(f'{{__name__=~"fd_c_\\\\d+_write_time_avg_us",{S}}}', "write {{__name__}} s{{slot}}")],
             unit="µs"), 8, 7)
    g.add(ts("WAL Persist Rate (kv/s)",
             [target(f'rate(wal_group_first_need_persist_kv_count_total{{{S}}}[{RATE}])',
                     "s{{slot}} {{instance}}")], unit="ops"), 8, 7)
    g.add(ts("Key Bucket Read/Write Bytes Rate",
             [target(f'rate({{__name__=~"fd_k_\\\\d+_read_bytes_total",{S}}}[{RATE}])', "read s{{slot}}"),
              target(f'rate({{__name__=~"fd_k_\\\\d+_write_bytes_total",{S}}}[{RATE}])', "write s{{slot}}")],
             unit="Bps"), 8, 7)
    g.add(ts("Chunk Data Read/Write Bytes Rate",
             [target(f'rate({{__name__=~"fd_c_\\\\d+_read_bytes_total",{S}}}[{RATE}])', "read s{{slot}}"),
              target(f'rate({{__name__=~"fd_c_\\\\d+_write_bytes_total",{S}}}[{RATE}])', "write s{{slot}}")],
             unit="Bps"), 8, 7)
    g.add(ts("FD Read/Write Count Rate",
             [target(f'rate({{__name__=~"fd_k_\\\\d+_read_count_total",{S}}}[{RATE}])', "k read s{{slot}}"),
              target(f'rate({{__name__=~"fd_k_\\\\d+_write_count_total",{S}}}[{RATE}])', "k write s{{slot}}"),
              target(f'rate({{__name__=~"fd_c_\\\\d+_write_count_total",{S}}}[{RATE}])', "c write s{{slot}}")],
             unit="ops"), 8, 7)


# ---- dashboard 3: per-slot cache / compression / replication ------------
# rows: Cache & LRU / Per-FD Compression / Big Values / Replication.
def build_slot_behavior(g):
    g.row("Cache & LRU — is caching working? (slot 0 & last)")
    g.add(ts("KV LRU Hit Ratio",
             [target(f'slot_kv_lru_hit_ratio{{{S}}}', "s{{slot}} {{instance}}")],
             unit="percentunit", minv=0), 8, 7)
    g.add(ts("KV LRU Hit / Miss Rate",
             [target(f'rate(slot_kv_lru_hit_total{{{S}}}[{RATE}])', "hit s{{slot}}"),
              target(f'rate(slot_kv_lru_miss_total{{{S}}}[{RATE}])', "miss s{{slot}}")], unit="ops"), 8, 7)
    g.add(ts("KV LRU Current Count",
             [target(f'slot_kv_lru_current_count_total{{{S}}}', "s{{slot}} {{instance}}")],
             unit="short"), 8, 7)
    g.add(ts("CV Encoded Length Avg (WAL vs LRU)",
             [target(f'slot_kv_wal_cv_encoded_length_avg{{{S}}}', "wal s{{slot}}"),
              target(f'slot_kv_lru_cv_encoded_length_avg{{{S}}}', "lru s{{slot}}")], unit="bytes"), 8, 7)
    g.add(ts("WAL Hit Rate",
             [target(f'rate(slot_kv_wal_hit_total{{{S}}}[{RATE}])', "s{{slot}} {{instance}}")],
             unit="ops"), 8, 7)
    g.add(ts("Key Bucket FD LRU Hit Ratio",
             [target(f'{{__name__=~"fd_k_\\\\d+_lru_hit_ratio",{S}}}', "{{__name__}} s{{slot}}")],
             unit="percentunit", minv=0), 8, 7)

    g.row("Per-FD Compression — is compression paying off? (slot 0 & last)")
    g.add(ts("Key Bucket After-Read Compress Ratio",
             [target(f'{{__name__=~"fd_k_\\\\d+_after_read_compress_ratio",{S}}}', "{{__name__}} s{{slot}}")],
             unit="percentunit", minv=0, desc="lower is better"), 8, 7)
    g.add(ts("Key Bucket Compress Cost Avg (µs)",
             [target(f'{{__name__=~"fd_k_\\\\d+_after_read_compress_time_avg_us",{S}}}',
                     "{{__name__}} s{{slot}}")], unit="µs"), 8, 7)
    g.add(ts("Key Bucket Read Decompress Avg (µs)",
             [target(f'{{__name__=~"fd_k_\\\\d+_after_lru_read_decompress_time_avg_us",{S}}}',
                     "{{__name__}} s{{slot}}")], unit="µs"), 8, 7)

    g.row("Big Values (slot 0 & last)")
    g.add(ts("Big Key Count",
             [target(f'big_key_count{{{S}}}', "s{{slot}} {{instance}}")], unit="short"), 8, 7)
    g.add(ts("Big String File Count",
             [target(f'big_string_files_count{{{S}}}', "files s{{slot}}"),
              target(f'big_string_files_delay_to_delete_count{{{S}}}', "delay-del s{{slot}}")],
             unit="short"), 8, 7)
    g.add(ts("Big String IO Rate (files/s)",
             [target(f'rate(big_string_files_read_file_count{{{S}}}[{RATE}])', "read s{{slot}}"),
              target(f'rate(big_string_files_write_file_count{{{S}}}[{RATE}])', "write s{{slot}}"),
              target(f'rate(big_string_files_delete_file_count{{{S}}}[{RATE}])', "delete s{{slot}}")],
             unit="ops"), 8, 7)

    g.row("Replication — are replicas in sync? (cluster / master-slave)")
    g.add(ts("Slave Link Up / Caught Up",
             [target(f'repl_as_slave_is_link_up{{{S}}}', "link-up s{{slot}} {{instance}}"),
              target(f'repl_as_slave_is_all_caught_up{{{S}}}', "caught-up s{{slot}} {{instance}}")],
             unit="short", desc="1 = true, 0 = false; empty on a non-replicating instance"), 8, 7)
    g.add(ts("Slave Catch-up Lag (ms)",
             [target(f'repl_as_slave_catch_up_last_time_millis_diff{{{S}}}', "s{{slot}} {{instance}}")],
             unit="ms"), 8, 7)
    g.add(ts("Slave Fetched Bytes Rate",
             [target(f'rate(repl_as_slave_fetched_bytes_total{{{S}}}[{RATE}])', "s{{slot}} {{instance}}")],
             unit="Bps"), 8, 7)
    g.add(ts("Master Repl Pair Size",
             [target(f'repl_as_master_repl_pair_size{{{S}}}', "s{{slot}} {{instance}}")], unit="short"), 8, 7)
    g.add(ts("Slave Binlog Offset (slave vs master)",
             [target(f'repl_as_slave_last_catch_up_binlog_offset{{{S}}}', "slave s{{slot}}"),
              target(f'repl_as_slave_master_binlog_current_offset{{{S}}}', "master s{{slot}}")],
             unit="bytes"), 8, 7)
    g.add(ts("Binlog Current Offset (from beginning)",
             [target(f'binlog_current_offset_from_the_beginning{{{S}}}', "s{{slot}} {{instance}}")],
             unit="bytes"), 8, 7)
    g.add(ts("Slot Last Seq",
             [target(f'slot_last_seq{{{S}}}', "s{{slot}} {{instance}}")], unit="short"), 8, 7)


def var(name, metric):
    return {
        "name": name, "type": "query", "datasource": DS,
        "definition": f"label_values({metric}, {name})",
        "query": {"qryType": 1, "query": f"label_values({metric}, {name})",
                  "refId": "StandardVariableQuery"},
        "multi": True, "includeAll": True, "allValue": ".*",
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "refresh": 2, "sort": 1,
    }


def make(uid, title, build, with_slot):
    _id[0] = 0
    target.n = 0
    g = Grid()
    build(g)
    tvars = [var("instance", "global_up_time")]
    if with_slot:
        tvars.append(var("slot", "slot_kv_lru_hit_ratio"))
    return {
        "id": None, "uid": uid, "title": title,
        "tags": ["velo", "redis", "kv"], "timezone": "browser", "schemaVersion": 39,
        "version": 1, "editable": True, "graphTooltip": 1, "refresh": "10s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {"list": tvars},
        "annotations": {"list": [{
            "builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "enable": True, "hide": True, "type": "dashboard",
            "name": "Annotations & Alerts", "iconColor": "rgba(0, 211, 255, 1)"}]},
        "panels": g.panels,
    }


DASHBOARDS = [
    ("velo-global", "Velo — Global", build_global, False),
    ("velo-slot-capacity", "Velo — Slot Capacity & Persistence", build_slot_capacity, True),
    ("velo-slot-behavior", "Velo — Slot Cache / Compression / Replication", build_slot_behavior, True),
]

for uid, title, build, with_slot in DASHBOARDS:
    d = make(uid, title, build, with_slot)
    body = json.dumps(d, indent=2)
    with open(f"grafana/dashboards/{uid}.json", "w") as f:
        f.write(body)
    with open(f"../doc/metrics/{uid}.json", "w") as f:
        f.write(body)
    n = len([p for p in d["panels"] if p["type"] != "row"])
    r = len([p for p in d["panels"] if p["type"] == "row"])
    print(f"{uid}: {n} panels, {r} rows")
