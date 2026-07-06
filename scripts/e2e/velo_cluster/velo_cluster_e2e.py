#!/usr/bin/env python3
import argparse
import pathlib
import sys
import time
from dataclasses import dataclass

import redis
from redis.crc import key_slot


TOTAL_CLIENT_SLOTS = 16384


@dataclass
class Node:
    idx: int
    node_id: str
    host: str
    port: int
    client: redis.Redis

    @property
    def label(self) -> str:
        return f"{self.node_id}@{self.host}:{self.port}"


class E2E:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.step = TOTAL_CLIENT_SLOTS // args.slot_number
        self.nodes = [
            Node(
                idx=i,
                node_id=f"velo-e2e-{i}",
                host=args.host,
                port=port,
                client=redis.Redis(
                    host=args.host,
                    port=port,
                    socket_connect_timeout=1.0,
                    socket_timeout=10.0,
                    decode_responses=True,
                ),
            )
            for i, port in enumerate(args.ports)
        ]
        self.transcript: list[str] = []
        self.bugs: list[str] = []
        self.version = 1
        self.did_migrate = False

    def log(self, message: str) -> None:
        line = f"[{time.strftime('%H:%M:%S')}] {message}"
        self.transcript.append(line)
        print(line, flush=True)

    def bug(self, message: str) -> None:
        self.bugs.append(message)
        self.log(f"BUG CANDIDATE: {message}")

    def wait_for_nodes(self) -> None:
        deadline = time.time() + self.args.timeout
        pending = {node.idx for node in self.nodes}
        while pending and time.time() < deadline:
            for idx in list(pending):
                node = self.nodes[idx]
                try:
                    if node.client.ping():
                        self.log(f"PING ok: {node.label}")
                        pending.remove(idx)
                except redis.RedisError:
                    pass
            if pending:
                time.sleep(0.25)
        if pending:
            raise RuntimeError(f"Timed out waiting for nodes: {sorted(pending)}")

    def raw(self, node: Node, *parts: object, expect_ok: bool = False) -> object:
        printable = " ".join(str(part).replace("\n", "\\n") for part in parts)
        self.log(f"> {node.label}: {printable}")
        try:
            result = node.client.execute_command(*parts)
        except redis.RedisError as exc:
            self.log(f"< {node.label}: ERROR {exc}")
            raise
        self.log(f"< {node.label}: {result!r}")
        if expect_ok and result not in ("OK", True):
            raise AssertionError(f"{node.label} expected OK for {printable}, got {result!r}")
        return result

    def clusterx(self, node: Node, *args: object, expect_ok: bool = False) -> object:
        return self.raw(node, "CLUSTERX", *args, expect_ok=expect_ok)

    def manage(self, node: Node, *args: object, expect_ok: bool = False) -> object:
        return self.raw(node, "MANAGE", *args, expect_ok=expect_ok)

    def slot_range_text(self, begin: int, end: int) -> str:
        return f"{begin}-{end}"

    def initial_nodes_args(self) -> str:
        ranges = [
            self.slot_range_text(0, self.step - 1),
            self.slot_range_text(self.step, (2 * self.step) - 1),
            self.slot_range_text(2 * self.step, TOTAL_CLIENT_SLOTS - 1),
        ]
        return "\n".join(
            [
                f"{self.nodes[0].node_id} {self.args.host} {self.nodes[0].port} master - {ranges[0]}",
                f"{self.nodes[1].node_id} {self.args.host} {self.nodes[1].port} master - {ranges[1]}",
                f"{self.nodes[2].node_id} {self.args.host} {self.nodes[2].port} master - {ranges[2]}",
                f"{self.nodes[3].node_id} {self.args.host} {self.nodes[3].port} slave {self.nodes[1].node_id}",
            ]
        )

    def failover_nodes_args(self) -> str:
        if self.did_migrate:
            node0_range = ""
            promoted_range = f"0-{(2 * self.step) - 1}"
        else:
            node0_range = f"0-{self.step - 1}"
            promoted_range = f"{self.step}-{(2 * self.step) - 1}"
        return "\n".join(
            [
                f"{self.nodes[0].node_id} {self.args.host} {self.nodes[0].port} master - {node0_range}".rstrip(),
                f"{self.nodes[3].node_id} {self.args.host} {self.nodes[3].port} master - {promoted_range}",
                f"{self.nodes[1].node_id} {self.args.host} {self.nodes[1].port} slave {self.nodes[3].node_id}",
                f"{self.nodes[2].node_id} {self.args.host} {self.nodes[2].port} master - {2 * self.step}-{TOTAL_CLIENT_SLOTS - 1}",
            ]
        )

    def broadcast_setnodes(self, nodes_args: str) -> None:
        self.version += 1
        for node in self.nodes:
            self.clusterx(node, "SETNODES", nodes_args, self.version, expect_ok=True)

    def broadcast_setslot(self, slot: int, node_id: str) -> None:
        self.version += 1
        for node in self.nodes:
            self.clusterx(node, "SETSLOT", slot, "NODE", node_id, self.version, expect_ok=True)

    def assert_contains(self, where: object, needle: str, message: str) -> None:
        if needle not in str(where):
            self.bug(f"{message}; missing {needle!r} in {where!r}")

    def assert_cluster_ok(self, node: Node) -> None:
        info = self.clusterx(node, "INFO")
        self.assert_contains(info, "cluster_state:ok", f"{node.label} cluster state is not ok")

    def find_key_for_slot(self, owner: Node, slot: int) -> str:
        for i in range(200000):
            key = f"velo-e2e:slot-{slot}:{i}"
            actual = key_slot(key.encode())
            if int(actual) == slot:
                server_actual = self.clusterx(owner, "KEYSLOT", key)
                if int(server_actual) != slot:
                    raise RuntimeError(
                        f"Local redis-py key slot disagrees with Velo: key={key} "
                        f"local={slot} server={server_actual}"
                    )
                return key
        raise RuntimeError(f"Could not find key for slot {slot}")

    def expect_moved(self, node: Node, command: str, *args: object) -> None:
        try:
            self.raw(node, command, *args)
        except redis.RedisError as exc:
            if "MOVED" in str(exc) or type(exc).__name__ == "MovedError":
                return
            raise
        self.bug(f"{node.label} accepted {command} for a slot it should not own")

    def wait_for_migration_success(self, node: Node) -> None:
        deadline = time.time() + self.args.timeout
        last_info = None
        while time.time() < deadline:
            last_info = self.clusterx(node, "INFO")
            if "migrating_state:success" in str(last_info):
                return
            time.sleep(1)
        self.bug(f"Migration did not reach success on {node.label}; last info={last_info!r}")

    def wait_for_value(self, node: Node, key: str, expected: str) -> None:
        deadline = time.time() + self.args.timeout
        last_result: object = None
        while time.time() < deadline:
            try:
                last_result = self.raw(node, "GET", key)
                if last_result == expected:
                    return
            except redis.RedisError as exc:
                last_result = f"{type(exc).__name__}: {exc}"
            time.sleep(1)
        self.bug(f"{node.label} did not expose replicated value for {key}; last={last_result!r}")

    def dump_metadata(self) -> None:
        for node in self.nodes:
            try:
                self.clusterx(node, "MYID")
                self.clusterx(node, "NODES")
                self.clusterx(node, "SLOTS")
                self.clusterx(node, "SHARDS")
                self.clusterx(node, "INFO")
            except redis.RedisError as exc:
                self.bug(f"Metadata dump failed for {node.label}: {exc}")

    def bootstrap_cluster(self) -> None:
        self.log("=== bootstrap cluster ===")
        self.broadcast_setnodes(self.initial_nodes_args())
        for node in self.nodes:
            actual = self.clusterx(node, "MYID")
            if actual != node.node_id:
                self.bug(f"{node.label} MYID mismatch: {actual!r}")
            self.assert_cluster_ok(node)
        self.dump_metadata()

    def routing_smoke(self) -> tuple[str, str]:
        self.log("=== routing smoke ===")
        slot0_key = self.find_key_for_slot(self.nodes[0], 0)
        slot1_key = self.find_key_for_slot(self.nodes[1], self.step)
        self.raw(self.nodes[0], "SET", slot0_key, "before-migrate", expect_ok=True)
        got = self.raw(self.nodes[0], "GET", slot0_key)
        if got != "before-migrate":
            self.bug(f"Owner read mismatch before migration: got {got!r}")
        self.expect_moved(self.nodes[0], "SET", slot1_key, "wrong-owner")
        self.raw(self.nodes[1], "SET", slot1_key, "before-failover", expect_ok=True)
        if self.args.run_failover == "true":
            self.wait_for_value(self.nodes[3], slot1_key, "before-failover")
        return slot0_key, slot1_key

    def migrate_first_block(self, slot0_key: str) -> None:
        if self.args.run_migration != "true":
            self.log("=== migration skipped ===")
            return

        self.log("=== migrate slot block 0 from node-0 to node-1 ===")
        source = self.nodes[0]
        target = self.nodes[1]
        self.manage(target, "SLOT", 0, "MIGRATE_FROM", source.host, source.port, "FORCE", expect_ok=True)
        self.clusterx(source, "MIGRATE", 0, target.node_id, expect_ok=True)
        self.wait_for_migration_success(source)
        self.broadcast_setslot(0, target.node_id)
        self.did_migrate = True
        self.assert_cluster_ok(target)
        self.expect_moved(source, "GET", slot0_key)
        got = self.raw(target, "GET", slot0_key)
        if got != "before-migrate":
            self.bug(f"Migrated key lost or stale on target: key={slot0_key} got={got!r}")
        self.wait_for_value(self.nodes[3], slot0_key, "before-migrate")
        self.dump_metadata()

    def failover_replica(self, slot1_key: str) -> None:
        if self.args.run_failover != "true":
            self.log("=== failover skipped ===")
            return

        self.log("=== failover node-3 replica of node-1 ===")
        replica = self.nodes[3]
        try:
            self.clusterx(replica, "FAILOVER", "FORCE", expect_ok=True)
        except redis.RedisError as exc:
            self.bug(f"CLUSTERX FAILOVER FORCE failed on {replica.label}: {exc}")
            return
        self.broadcast_setnodes(self.failover_nodes_args())
        for node in self.nodes:
            self.assert_cluster_ok(node)
        got = self.raw(replica, "GET", slot1_key)
        if got != "before-failover":
            self.bug(f"Failover promoted node cannot read replicated key: key={slot1_key} got={got!r}")
        self.raw(replica, "SET", slot1_key, "after-failover", expect_ok=True)
        self.dump_metadata()

    def log_tails(self) -> None:
        base = pathlib.Path(self.args.base_dir)
        self.log("=== server log tails ===")
        for path in sorted(base.glob("node-*/server.log")):
            print(f"--- {path} ---", flush=True)
            try:
                lines = path.read_text(errors="replace").splitlines()
            except OSError as exc:
                print(f"could not read log: {exc}", flush=True)
                continue
            for line in lines[-80:]:
                print(line, flush=True)

    def run(self) -> int:
        try:
            self.wait_for_nodes()
            self.bootstrap_cluster()
            slot0_key, slot1_key = self.routing_smoke()
            self.migrate_first_block(slot0_key)
            self.failover_replica(slot1_key)
        except Exception as exc:
            self.bug(f"Unhandled e2e exception: {type(exc).__name__}: {exc}")
        finally:
            if self.bugs:
                self.log_tails()

        if self.bugs:
            self.log("=== bug candidates ===")
            for bug in self.bugs:
                print(f"- {bug}", flush=True)
            return 1
        self.log("All Velo cluster e2e checks passed")
        return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual Velo cluster e2e driver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--ports", nargs="+", type=int, required=True)
    parser.add_argument("--base-dir", required=True)
    parser.add_argument("--timeout", type=int, default=90)
    parser.add_argument("--slot-number", type=int, default=4)
    parser.add_argument("--run-migration", default="true", choices=("true", "false"))
    parser.add_argument("--run-failover", default="true", choices=("true", "false"))
    args = parser.parse_args()
    if len(args.ports) < 4:
        parser.error("--ports must contain at least four ports")
    if TOTAL_CLIENT_SLOTS % args.slot_number != 0:
        parser.error("--slot-number must divide 16384")
    return args


if __name__ == "__main__":
    sys.exit(E2E(parse_args()).run())
