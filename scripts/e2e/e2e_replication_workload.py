#!/usr/bin/env python3
"""
Workload generator + data verifier for the e2e replication smoke test.

Called by scripts/e2e/e2e_2n_replication.sh after both master/slave pairs are up and
the slaves have completed initial sync. It:

  1. Drives an exact-count mixed read/write workload against both the Velo master
     and the Redis master using the same key/value command stream.
  2. Writes use fixed-width keys: key:000000000001 through key:{N}.
  3. After the run, writes a post-workload marker and waits for it on both slaves
     (guarantees replication has drained to a known point).
  4. Snapshots the full key->value map on master and slave for both systems via
     SCAN and compares replication plus Velo-vs-Redis compatibility.

Exits 0 only if both replication pairs agree and Velo agrees with Redis.
"""

import argparse
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from math import ceil

import redis as redis_lib


# ------------------------- workload planning -------------------------

@dataclass(frozen=True)
class WorkloadPlan:
    target_qps: int
    duration_s: int
    read_ratio: float

    def __post_init__(self):
        if self.target_qps <= 0:
            raise ValueError("target_qps must be positive")
        if self.duration_s <= 0:
            raise ValueError("duration_s must be positive")
        if self.read_ratio < 0 or self.read_ratio > 1:
            raise ValueError("read_ratio must be between 0 and 1")

        total_ops = self.target_qps * self.duration_s
        total_reads = round(total_ops * self.read_ratio)
        total_writes = total_ops - total_reads
        object.__setattr__(self, "total_ops", total_ops)
        object.__setattr__(self, "total_reads", total_reads)
        object.__setattr__(self, "total_writes", total_writes)


def fixed_key(index: int) -> str:
    if index <= 0:
        raise ValueError("key index must be positive")
    return f"key:{index:012d}"


def fixed_value(index: int, data_length: int) -> str:
    base = f"value:{index:012d}:"
    if data_length <= len(base):
        return base[:data_length]
    return base + ("x" * (data_length - len(base)))


# ------------------------- big string workload -------------------------

# Key prefix used for big-string SET/GET coverage. Big strings in Velo take a
# different code path (on-disk BigStringFiles + XBigStrings binlog + a separate
# file fetch during replication), so they need dedicated e2e coverage.
BIG_STRING_KEY_PREFIX = "kerry-test-big-string-"

# Sizes (in bytes) chosen to exercise both Velo big-string branches:
#   * 256 KiB exactly: size >= bigStringNoMemoryCopySize (256 KiB) makes it a
#     big string, but length <= bigStringNoCompressMinSize (256 KiB) so it is
#     still compressed.
#   * > 256 KiB       : big string stored raw (no compression).
BIG_STRING_SIZES = [
    256 * 1024,
    384 * 1024,
    512 * 1024,
    1024 * 1024,
]


def big_string_key(index: int) -> str:
    if index <= 0:
        raise ValueError("big string index must be positive")
    return f"{BIG_STRING_KEY_PREFIX}{index:04d}"


def big_string_size(index: int) -> int:
    return BIG_STRING_SIZES[(index - 1) % len(BIG_STRING_SIZES)]


def big_string_value(index: int, size: int) -> bytes:
    """Deterministic big-string payload of exactly `size` bytes."""
    header = f"big:{index:04d}:".encode("utf-8")
    if size <= len(header):
        return header[:size]
    chunk = f"<kerry-{index:04d}>".encode("utf-8")
    body = (chunk * (size // len(chunk) + 2))[: size - len(header)]
    return header + body


# ------------------------- workload -------------------------

class WorkloadStats:
    def __init__(self):
        self.reads = 0
        self.writes = 0
        self.errors = 0
        self._lock = threading.Lock()

    def add_reads(self, count: int):
        with self._lock:
            self.reads += count

    def add_writes(self, count: int):
        with self._lock:
            self.writes += count

    def inc_error(self):
        with self._lock:
            self.errors += 1

    def snapshot(self):
        with self._lock:
            return self.writes, self.reads, self.errors


def op_range_for_client(total_ops: int, clients: int, client_index: int) -> tuple[int, int]:
    start = (total_ops * client_index) // clients + 1
    end = (total_ops * (client_index + 1)) // clients
    return start, end


def write_count_after_op(plan: WorkloadPlan, op_index: int) -> int:
    if op_index <= 0:
        return 0
    return ceil(op_index * plan.total_writes / plan.total_ops)


def run_client_worker(client_id: int, host: str, velo_port: int, redis_port: int, plan: WorkloadPlan,
                      data_length: int, pipeline_batch: int, clients: int, stats: WorkloadStats):
    start_op, end_op = op_range_for_client(plan.total_ops, clients, client_id)
    client_ops = end_op - start_op + 1
    client_qps = plan.target_qps / float(clients)
    velo_conn = redis_lib.Redis(host=host, port=velo_port, socket_timeout=60, socket_connect_timeout=5)
    redis_conn = redis_lib.Redis(host=host, port=redis_port, socket_timeout=60, socket_connect_timeout=5)
    processed = 0
    op_index = start_op
    start = time.time()
    try:
        while op_index <= end_op:
            batch_limit = min(pipeline_batch, end_op - op_index + 1)
            velo_pipe = velo_conn.pipeline(transaction=False)
            redis_pipe = redis_conn.pipeline(transaction=False)
            batch_reads = 0
            batch_writes = 0

            for _ in range(batch_limit):
                writes_before = write_count_after_op(plan, op_index - 1)
                writes_after = write_count_after_op(plan, op_index)
                if writes_after > writes_before:
                    key = fixed_key(writes_after)
                    value = fixed_value(writes_after, data_length)
                    velo_pipe.set(key, value)
                    redis_pipe.set(key, value)
                    batch_writes += 1
                else:
                    read_index = 1 + ((op_index * 1_103_515_245 + 12_345) % max(writes_before, 1))
                    key = fixed_key(read_index)
                    velo_pipe.get(key)
                    redis_pipe.get(key)
                    batch_reads += 1
                op_index += 1
                processed += 1

            try:
                velo_pipe.execute()
                redis_pipe.execute()
            except Exception as e:
                stats.inc_error()
                print(f"  [client-{client_id}] batch error after {processed}/{client_ops} ops: "
                      f"{type(e).__name__}: {e}", file=sys.stderr)
                raise

            stats.add_writes(batch_writes)
            stats.add_reads(batch_reads)

            target_elapsed = processed / client_qps
            elapsed = time.time() - start
            sleep_s = target_elapsed - elapsed
            if sleep_s > 0:
                time.sleep(sleep_s)
    finally:
        velo_conn.close()
        redis_conn.close()


def run_shared_workload(host: str, velo_port: int, redis_port: int, plan: WorkloadPlan,
                        data_length: int, pipeline_batch: int, clients: int, stats: WorkloadStats):
    """Run one deterministic command stream through multiple rate-limited clients."""
    stop_progress = threading.Event()
    start = time.time()

    def progress_monitor():
        next_progress_s = 2
        while not stop_progress.wait(0.2):
            elapsed = time.time() - start
            if elapsed >= next_progress_s:
                writes, reads, errors = stats.snapshot()
                print(f"  [t={next_progress_s}s] shared: w={writes} r={reads} err={errors}")
                next_progress_s += 2

    monitor = threading.Thread(target=progress_monitor, name="workload-progress", daemon=True)
    monitor.start()
    try:
        with ThreadPoolExecutor(max_workers=clients, thread_name_prefix="shared-client") as pool:
            futures = [
                pool.submit(run_client_worker, i, host, velo_port, redis_port, plan,
                            data_length, pipeline_batch, clients, stats)
                for i in range(clients)
            ]
            for future in futures:
                future.result()
    finally:
        stop_progress.set()
        monitor.join(timeout=1)


# ------------------------- big string workload + verification -------------------------

def write_big_strings(host: str, velo_port: int, redis_port: int, count: int) -> list:
    """Write `count` big-string keys to both Velo and Redis masters.

    Returns a list of (key, size, value_bytes) tuples for later verification.
    """
    items = []
    if count <= 0:
        return items
    velo_conn = redis_lib.Redis(host=host, port=velo_port, socket_timeout=120, socket_connect_timeout=5)
    redis_conn = redis_lib.Redis(host=host, port=redis_port, socket_timeout=120, socket_connect_timeout=5)
    try:
        for i in range(1, count + 1):
            key = big_string_key(i)
            size = big_string_size(i)
            value = big_string_value(i, size)
            velo_conn.set(key, value)
            redis_conn.set(key, value)
            items.append((key, size, value))
            print(f"  wrote {key} ({size} bytes = {size / 1024:.0f} KiB)")
    finally:
        velo_conn.close()
        redis_conn.close()
    return items


def verify_big_strings(host: str, velo_master_port: int, velo_slave_port: int,
                       redis_master_port: int, redis_slave_port: int,
                       items: list, timeout_s: float = 180, poll_interval: float = 1.0) -> dict:
    """Verify big strings replicated to all 4 nodes.

    Big strings replicate via a separate file-fetch path in Velo, so slaves are
    polled until the values appear (or timeout_s elapses).
    """
    result = {
        "checked": 0,
        "missing": 0,
        "mismatch": 0,
        "samples": [],
    }
    if not items:
        return result

    conns = {
        "velo_master": redis_lib.Redis(host=host, port=velo_master_port, socket_timeout=120, socket_connect_timeout=5),
        "velo_slave": redis_lib.Redis(host=host, port=velo_slave_port, socket_timeout=120, socket_connect_timeout=5),
        "redis_master": redis_lib.Redis(host=host, port=redis_master_port, socket_timeout=120, socket_connect_timeout=5),
        "redis_slave": redis_lib.Redis(host=host, port=redis_slave_port, socket_timeout=120, socket_connect_timeout=5),
    }
    deadline = time.time() + timeout_s
    try:
        pending = list(items)
        while pending and time.time() < deadline:
            still_pending = []
            for item in pending:
                key, size, expected = item
                got = {}
                for name, conn in conns.items():
                    try:
                        got[name] = conn.get(key)
                    except Exception:
                        got[name] = None
                if any(v is None for v in got.values()):
                    still_pending.append(item)
                    continue

                result["checked"] += 1
                if any(v != expected for v in got.values()):
                    result["mismatch"] += 1
                    if len(result["samples"]) < 10:
                        offenders = {n: len(v) for n, v in got.items()}
                        result["samples"].append((key, size, "value_mismatch", offenders))
            pending = still_pending
            if pending:
                time.sleep(poll_interval)

        for key, size, _ in pending:
            result["missing"] += 1
            if len(result["samples"]) < 10:
                result["samples"].append((key, size, "missing", None))
    finally:
        for conn in conns.values():
            conn.close()
    return result


# ------------------------- verification -------------------------

def snapshot_all(host: str, port: int, retries: int = 5) -> dict:
    """Full key->value snapshot via SCAN. Retries transient read-gate errors."""
    conn = redis_lib.Redis(host=host, port=port, socket_timeout=60, socket_connect_timeout=5)
    result = {}
    cursor = 0
    try:
        while True:
            batch = None
            for attempt in range(retries):
                try:
                    batch = conn.scan(cursor=cursor, count=500)
                    break
                except Exception:
                    if attempt == retries - 1:
                        raise
                    time.sleep(0.5)
            cursor, keys = batch
            if keys:
                pipe = conn.pipeline(transaction=False)
                for k in keys:
                    pipe.get(k)
                values = None
                for attempt in range(retries):
                    try:
                        values = pipe.execute()
                        break
                    except Exception as e:
                        if attempt == retries - 1:
                            print(f"  snapshot: pipeline GET failed: {type(e).__name__}: {e}", file=sys.stderr)
                            raise
                        time.sleep(0.5)
                for k, v in zip(keys, values):
                    kk = k.decode() if isinstance(k, bytes) else k
                    vv = v.decode() if isinstance(v, bytes) else v
                    result[kk] = vv if vv is not None else "<nil>"
            if cursor == 0:
                break
    finally:
        conn.close()
    return result


def compare_maps(left: dict, right: dict) -> tuple:
    """Return (only_left, only_right, mismatch_count, sample_offenders)."""
    only_left = [k for k in left if k not in right]
    only_right = [k for k in right if k not in left]
    mismatch = [(k, left[k], right[k]) for k in left if k in right and left[k] != right[k]]
    return len(only_left), len(only_right), len(mismatch), mismatch[:10]


def print_offenders(label: str, samples):
    if not samples:
        return
    print(f"  {label} offending samples:")
    for k, lv, rv in samples:
        left = lv[:80] + "..." if len(lv) > 80 else lv
        right = rv[:80] + "..." if len(rv) > 80 else rv
        print(f"    {k} -> left={left} right={right}")


def wait_key_value(host: str, port: int, key: str, expected: str, timeout_s: float = 90) -> bool:
    deadline = time.time() + timeout_s
    conn = redis_lib.Redis(host=host, port=port, socket_timeout=2, socket_connect_timeout=2)
    try:
        while time.time() < deadline:
            try:
                v = conn.get(key)
                actual = v.decode() if isinstance(v, bytes) else v
                if actual == expected:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False
    finally:
        conn.close()


def pipeline_get(conn, keys):
    pipe = conn.pipeline(transaction=False)
    for key in keys:
        pipe.get(key)
    return pipe.execute()


def verify_expected_range(host: str, velo_master_port: int, velo_slave_port: int,
                          redis_master_port: int, redis_slave_port: int,
                          total_writes: int, data_length: int, batch_size: int) -> dict:
    conns = {
        "velo_master": redis_lib.Redis(host=host, port=velo_master_port, socket_timeout=60, socket_connect_timeout=5),
        "velo_slave": redis_lib.Redis(host=host, port=velo_slave_port, socket_timeout=60, socket_connect_timeout=5),
        "redis_master": redis_lib.Redis(host=host, port=redis_master_port, socket_timeout=60, socket_connect_timeout=5),
        "redis_slave": redis_lib.Redis(host=host, port=redis_slave_port, socket_timeout=60, socket_connect_timeout=5),
    }
    result = {
        "checked": 0,
        "missing": 0,
        "mismatch": 0,
        "samples": [],
    }
    try:
        for start in range(1, total_writes + 1, batch_size):
            end = min(total_writes, start + batch_size - 1)
            keys = [fixed_key(i) for i in range(start, end + 1)]
            expected_values = [fixed_value(i, data_length) for i in range(start, end + 1)]
            batch_values = {name: pipeline_get(conn, keys) for name, conn in conns.items()}

            for offset, key in enumerate(keys):
                expected = expected_values[offset]
                decoded = {}
                for name, values in batch_values.items():
                    value = values[offset]
                    decoded[name] = value.decode() if isinstance(value, bytes) else value

                if any(value is None for value in decoded.values()):
                    result["missing"] += 1
                    if len(result["samples"]) < 10:
                        result["samples"].append((key, "missing", decoded))
                elif any(value != expected for value in decoded.values()):
                    result["mismatch"] += 1
                    if len(result["samples"]) < 10:
                        result["samples"].append((key, expected, decoded))
                result["checked"] += 1

            if result["checked"] % 50_000 == 0 or result["checked"] == total_writes:
                print(f"  verified {result['checked']}/{total_writes} keys")
    finally:
        for conn in conns.values():
            conn.close()
    return result


# ------------------------- main -------------------------

def main():
    p = argparse.ArgumentParser(description="e2e replication workload + verifier")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--velo-master-port", type=int, required=True)
    p.add_argument("--velo-slave-port", type=int, required=True)
    p.add_argument("--redis-master-port", type=int, required=True)
    p.add_argument("--redis-slave-port", type=int, required=True)
    p.add_argument("--run-seconds", type=int, default=30)
    p.add_argument("--target-qps", type=int, default=1000)
    p.add_argument("--worker-threads", type=int, default=10)
    p.add_argument("--read-ratio", type=float, default=0.8)
    p.add_argument("--data-length", type=int, default=100)
    p.add_argument("--settle-seconds", type=int, default=10)
    p.add_argument("--pipeline-batch", type=int, default=100)
    p.add_argument("--big-string-count", type=int, default=4,
                   help="number of big-string keys (>= 256 KiB) to write and verify; 0 skips")
    args = p.parse_args()

    host = args.host
    suffix = str(int(time.time() * 1000))
    plan = WorkloadPlan(args.target_qps, args.run_seconds, args.read_ratio)

    sample_key = fixed_key(1)
    sample = fixed_value(1, args.data_length)
    print(f"sample key   = {sample_key} ({len(sample_key.encode('utf-8'))} bytes)")
    print(f"sample value = {len(sample)} chars: {sample[:200]}{'...' if len(sample) > 200 else ''}")
    print()
    print(f"---- workload: {args.target_qps} qps, {args.read_ratio} read ratio, "
          f"{args.run_seconds}s ----")
    print(f"  planned ops: total={plan.total_ops} writes={plan.total_writes} reads={plan.total_reads}")
    print(f"  clients    : {args.worker_threads}, avg qps/client={args.target_qps / float(args.worker_threads):.2f}")
    print(f"  key range  : {fixed_key(1)} - {fixed_key(plan.total_writes)}")

    stats = WorkloadStats()
    start = time.time()
    run_shared_workload(host, args.velo_master_port, args.redis_master_port, plan,
                        args.data_length, args.pipeline_batch, args.worker_threads, stats)
    workload_ms = int((time.time() - start) * 1000)
    print(f"  workload finished in {workload_ms} ms")

    # ---- big strings (separate Velo code path: on-disk files + separate repl fetch) ----
    big_string_items = []
    if args.big_string_count > 0:
        print()
        print(f"---- big strings: {args.big_string_count} keys, prefix={BIG_STRING_KEY_PREFIX} ----")
        big_string_items = write_big_strings(host, args.velo_master_port, args.redis_master_port,
                                             args.big_string_count)

    # ---- drain ----
    print()
    print(f"---- waiting {args.settle_seconds}s for replication to drain ----")
    time.sleep(args.settle_seconds)

    # ---- post-workload marker: guarantees slaves caught up to a known point ----
    print("---- writing post-workload markers and waiting for slaves ----")
    after_key = "marker:after"
    after_val = f"after-{suffix}"
    rc = redis_lib.Redis(host=host, port=args.velo_master_port)
    rc.set(after_key, after_val)
    rc.close()
    rc = redis_lib.Redis(host=host, port=args.redis_master_port)
    rc.set(after_key, after_val)
    rc.close()

    if not wait_key_value(host, args.velo_slave_port, after_key, after_val):
        print("FAIL: velo slave did not see post-workload marker (still behind)", file=sys.stderr)
        return 1
    if not wait_key_value(host, args.redis_slave_port, after_key, after_val):
        print("FAIL: redis slave did not see post-workload marker (still behind)", file=sys.stderr)
        return 1
    print("  both slaves confirmed caught up past post-workload marker")

    # ---- deterministic key-range compare ----
    print()
    print("---- verification ----")
    range_result = verify_expected_range(
        host,
        args.velo_master_port,
        args.velo_slave_port,
        args.redis_master_port,
        args.redis_slave_port,
        plan.total_writes,
        args.data_length,
        args.pipeline_batch,
    )

    # ---- big string compare (polled, since big strings use a separate repl fetch path) ----
    big_string_result = {"checked": 0, "missing": 0, "mismatch": 0, "samples": []}
    if big_string_items:
        print("  verifying big strings ...")
        big_string_result = verify_big_strings(
            host,
            args.velo_master_port,
            args.velo_slave_port,
            args.redis_master_port,
            args.redis_slave_port,
            big_string_items,
        )

    writes, reads, errors = stats.snapshot()
    stats_ok = writes == plan.total_writes and reads == plan.total_reads and errors == 0
    range_ok = (range_result["checked"] == plan.total_writes
                and range_result["missing"] == 0
                and range_result["mismatch"] == 0)
    big_string_ok = (big_string_result["checked"] == len(big_string_items)
                     and big_string_result["missing"] == 0
                     and big_string_result["mismatch"] == 0)

    print()
    print("==== RESULTS ====")
    print(f"expected range : {fixed_key(1)} - {fixed_key(plan.total_writes)}")
    print(f"range compare  : checked={range_result['checked']} "
          f"missing={range_result['missing']} mismatch={range_result['mismatch']}")
    print(f"ops            : shared(w={writes} r={reads} err={errors})")

    if range_result["samples"]:
        print("  range offending samples:")
        for key, expected, decoded in range_result["samples"]:
            print(f"    {key} expected={expected} actual={decoded}")

    if big_string_items:
        print(f"big string     : checked={big_string_result['checked']}/{len(big_string_items)} "
              f"missing={big_string_result['missing']} mismatch={big_string_result['mismatch']}")
        if big_string_result["samples"]:
            print("  big string offending samples:")
            for key, size, reason, detail in big_string_result["samples"]:
                print(f"    {key} ({size} bytes) {reason} {detail}")

    print()
    print(f"VELO_REDIS_REPL_RANGE      : {'PASS' if range_ok else 'FAIL'}")
    print(f"VELO_REDIS_REPL_BIG_STRING : {'PASS' if big_string_ok else 'FAIL'}")
    print(f"OPS_COUNT                  : {'PASS' if stats_ok else 'FAIL'}")
    return 0 if (range_ok and stats_ok and big_string_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
