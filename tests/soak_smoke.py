#!/usr/bin/env python3
"""Bounded-state concurrent strict WAL/hybrid soak with maintenance and process crashes.

python3 tests/soak_smoke.py --binary target/release/ffkv --duration 12 --json soak.json
--duration is workload seconds PER storage mode; recovery/restore add wall time.
A short run is not seven-day evidence. SIGKILL is not a power-loss simulation.
"""
import argparse
from collections import Counter, deque
import contextlib
import math
from pathlib import Path
import shutil
import subprocess
import socket
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from runtime import (Client, OwnedServer, ResponseError, current_rss, environment,
                     mapping, version, write_report)


class Counters:
    def __init__(self):
        self.lock = threading.Lock()
        self.values = Counter()
        self.errors = []

    def add(self, name, amount=1):
        with self.lock:
            self.values[name] += amount

    def fail(self, error):
        with self.lock:
            self.values["failures"] += 1
            if len(self.errors) < 32:
                self.errors.append(f"{type(error).__name__}: {error}")


class Connections:
    """Interrupt active socket reads before joining any workload thread."""
    def __init__(self):
        self.lock = threading.Lock()
        self.clients = set()

    @contextlib.contextmanager
    def open(self, address, timeout):
        with Client(address, timeout) as client:
            with self.lock:
                self.clients.add(client)
            try:
                yield client
            finally:
                with self.lock:
                    self.clients.discard(client)

    def interrupt(self):
        with self.lock:
            for client in self.clients:
                with contextlib.suppress(OSError):
                    client.socket.shutdown(socket.SHUT_RDWR)


class Worker:
    """One bounded list, one atomic tuple and at most one native job per worker."""
    def __init__(self, index, args, counters, connections):
        self.index, self.args, self.counters = index, args, counters
        self.connections = connections
        self.prefix = f"soak:{index}:".encode()
        self.sequence = 0
        self.string_ack = 0
        self.string_attempt = 0
        self.list_state = deque()
        self.list_candidate = None
        self.native = None
        self.stale = None
        self.payload = bytes([index % 256]) * args.payload_bytes

    def key(self, suffix):
        return self.prefix + suffix.encode()

    def counted(self, client, *command):
        self.counters.add("commands_attempted")
        result = client.command(*command)
        self.counters.add("commands_completed")
        return result

    def reject_stale(self, client, receipt):
        try:
            self.counted(client, "FQ.ACK", self.key("native"), receipt)
        except ResponseError:
            self.counters.add("stale_receipts_rejected")
        else:
            raise AssertionError("stale native receipt was accepted")

    def cycle(self, client):
        self.sequence += 1
        value = str(self.sequence).encode()
        self.string_attempt = self.sequence
        assert self.counted(client, "SET", self.key("string"), value) == b"OK"
        self.string_ack = self.sequence
        assert self.counted(client, "MSET", self.key("tuple:a"), value,
                            self.key("tuple:b"), value) == b"OK"
        pair = self.counted(client, "MGET", self.key("tuple:a"), self.key("tuple:b"))
        assert pair == [value, value], ("torn atomic tuple", pair)
        # A bounded FIFO with large disk-offloaded bodies, not an unbounded producer.
        if len(self.list_state) < self.args.list_bound:
            body = value + b":" + self.payload
            self.list_candidate = deque(self.list_state)
            self.list_candidate.appendleft(body)
            result = self.counted(client, "LPUSH", self.key("list"), body)
            assert result == len(self.list_candidate), ("list length", result)
        else:
            self.list_candidate = deque(self.list_state)
            expected = self.list_candidate.pop()
            assert self.counted(client, "RPOP", self.key("list")) == expected
        self.list_state, self.list_candidate = self.list_candidate, None

        # One token covers an enqueue whose reply may be lost. No list command is retried.
        token = value
        self.native = {"token": token, "payload": self.payload, "id": None}
        queue = self.key("native")
        result = mapping(self.counted(client, "FQ.ENQUEUE", queue, self.payload,
                                      "IDEMPOTENCY", token, "MAXATTEMPTS", 2))
        assert result[b"created"] == 1, result
        self.native["id"] = result[b"job_id"]
        duplicate = mapping(self.counted(client, "FQ.ENQUEUE", queue, self.payload,
                                         "IDEMPOTENCY", token))
        assert duplicate[b"created"] == 0 and duplicate[b"job_id"] == result[b"job_id"]
        self.counters.add("native_idempotent_retries")
        rows = self.counted(client, "FQ.CLAIM", queue, "VISIBILITY", 300000)
        assert len(rows) == 1, rows
        row = mapping(rows[0])
        assert row[b"payload"] == self.payload and row[b"job_id"] == result[b"job_id"]
        if self.stale is not None:
            self.reject_stale(client, self.stale)
        receipt = row[b"receipt"]
        extended = self.counted(client, "FQ.EXTEND", queue, receipt, 600000)
        assert isinstance(extended, int) and extended > row[b"deadline_ms"], extended
        assert self.counted(client, "FQ.NACK", queue, receipt, "DELAY", 0) == b"OK"
        self.reject_stale(client, receipt)
        rows = self.counted(client, "FQ.CLAIM", queue, "VISIBILITY", 300000)
        assert len(rows) == 1
        again = mapping(rows[0])
        assert again[b"job_id"] == row[b"job_id"] and again[b"receipt"] != receipt
        assert again[b"attempts"] == 2 and again[b"payload"] == self.payload
        self.reject_stale(client, receipt)
        if self.sequence % 8 == 0:
            assert self.counted(client, "FQ.NACK", queue, again[b"receipt"], "DELAY", 0) == b"OK"
            self.reject_stale(client, again[b"receipt"])
            dead = mapping(self.counted(client, "FQ.GET", queue, again[b"job_id"]))
            assert dead[b"state"] == b"dead" and dead[b"payload"] == self.payload
            self.counters.add("dead_state_checks")
            assert self.counted(client, "FQ.RETRY", queue, again[b"job_id"]) == b"OK"
            retried = self.counted(client, "FQ.CLAIM", queue, "VISIBILITY", 300000)
            assert len(retried) == 1
            fresh = mapping(retried[0])
            assert fresh[b"job_id"] == again[b"job_id"] and fresh[b"attempts"] == 1
            assert fresh[b"receipt"] != again[b"receipt"]
            self.reject_stale(client, again[b"receipt"])
            again = fresh
            self.counters.add("dead_redrives")
        assert self.counted(client, "FQ.ACK", queue, again[b"receipt"]) == b"OK"
        self.stale = again[b"receipt"]
        self.reject_stale(client, self.stale)
        # Terminal idempotency is checked before deleting the queue to bound metadata.
        terminal = mapping(self.counted(client, "FQ.ENQUEUE", queue, self.payload,
                                        "IDEMPOTENCY", token))
        assert terminal[b"job_id"] == result[b"job_id"] and terminal[b"created"] == 0
        if self.sequence % 8 == 0:
            scheduled_token = b"scheduled:" + token
            self.native = {"token": scheduled_token, "payload": self.payload, "id": None}
            future = mapping(self.counted(client, "FQ.ENQUEUE", queue, self.payload,
                                          "IDEMPOTENCY", scheduled_token, "DELAY", 86400000))
            self.native["id"] = future[b"job_id"]
            scheduled = mapping(self.counted(client, "FQ.GET", queue, future[b"job_id"]))
            assert scheduled[b"state"] == b"scheduled" and scheduled[b"payload"] == self.payload
            self.counters.add("scheduled_state_checks")
            assert self.counted(client, "FQ.CANCEL", queue, future[b"job_id"]) == b"OK"
            assert self.counted(client, "FQ.GET", queue, future[b"job_id"]) is None
            self.counters.add("scheduled_cancellations")
        self.native = None
        assert self.counted(client, "DEL", queue) == 1
        self.counters.add("cycles_completed")

    def run(self, address, stop, crashing):
        try:
            with self.connections.open(address, self.args.timeout) as client:
                while not stop.is_set():
                    self.cycle(client)
        except (OSError, ConnectionError) as error:
            if crashing.is_set() or stop.is_set():
                self.counters.add("unknown_outcome_connections")
            else:
                self.counters.fail(error)
                stop.set()
        except Exception as error:
            self.counters.fail(error)
            stop.set()

    def recover(self, client):
        value = client.command("GET", self.key("string"))
        actual = int(value) if value is not None else 0
        assert self.string_ack <= actual <= self.string_attempt, (
            "acknowledged strict string lost or impossible future value", actual,
            self.string_ack, self.string_attempt)
        pair = client.command("MGET", self.key("tuple:a"), self.key("tuple:b"))
        assert pair[0] == pair[1], ("recovered torn atomic tuple", pair)
        actual_list = client.command("LRANGE", self.key("list"), 0, -1)
        candidates = [list(self.list_state)]
        if self.list_candidate is not None:
            candidates.append(list(self.list_candidate))
        assert actual_list in candidates, "list recovery is neither pre-command nor post-command state"
        self.list_state, self.list_candidate = deque(actual_list), None
        self.counters.add("list_recovery_checks")
        if self.native is not None:
            queue, pending = self.key("native"), self.native
            duplicate = mapping(client.command("FQ.ENQUEUE", queue, pending["payload"],
                                               "IDEMPOTENCY", pending["token"]))
            if pending["id"] is not None:
                assert duplicate[b"job_id"] == pending["id"] and duplicate[b"created"] == 0, (
                    "native acknowledged idempotency did not survive recovery", duplicate)
            self.counters.add("native_recovery_idempotent_retries")
            page = mapping(client.command("FQ.JOBS", queue, 0, "COUNT", 2))
            assert page[b"cursor"] == b"0" and len(page[b"jobs"]) <= 1, page
            for metadata in page[b"jobs"]:
                job = mapping(metadata)
                assert job[b"job_id"] == duplicate[b"job_id"], job
                body = mapping(client.command("FQ.GET", queue, job[b"job_id"]))
                assert body[b"payload"] == self.payload
                client.command("FQ.CANCEL", queue, job[b"job_id"])
            self.native = None
        client.command("DEL", self.key("native"))
        self.counters.add("recovery_checks")


def observer(address, workers, args, counters, stop, crashing, connections):
    try:
        with connections.open(address, args.timeout) as client:
            while not stop.is_set():
                for worker in workers:
                    pair = client.command("MGET", worker.key("tuple:a"), worker.key("tuple:b"))
                    assert pair[0] == pair[1], ("concurrent reader saw torn tuple", pair)
                    counters.add("concurrent_tuple_reads")
                stop.wait(.001)
    except (OSError, ConnectionError) as error:
        if crashing.is_set() or stop.is_set():
            counters.add("unknown_observer_connections")
        else:
            counters.fail(error)
            stop.set()
    except Exception as error:
        counters.fail(error)
        stop.set()


def maintenance(address, args, counters, stop, crashing, connections, storage):
    try:
        with connections.open(address, args.timeout) as client:
            while not stop.wait(args.maintenance_interval):
                operations = [("FFKV.CHECKPOINT", "checkpoints")]
                if storage == "hybrid":
                    operations.append(("FFKV.COMPACT", "compactions"))
                for command, name in operations:
                    client.command(command)
                    counters.add(name)
    except (OSError, ConnectionError) as error:
        if crashing.is_set() or stop.is_set():
            counters.add("unknown_maintenance_outcomes")
        else:
            counters.fail(error)
            stop.set()
    except Exception as error:
        counters.fail(error)
        stop.set()


def run_mode(args, binary, directory, storage):
    counters = Counters()
    connections = Connections()
    workers = [Worker(i, args, counters, connections) for i in range(args.workers)]
    extra = ["--inline-threshold", "1024", "--cache-bytes", str(args.cache_bytes),
             "--segment-bytes", str(args.segment_bytes), "--shards", str(args.shards)]
    server = OwnedServer(binary, directory, mode="strict", storage=storage,
                         timeout=args.timeout, extra_args=extra)
    report = {"storage": storage, "durability": "strict", "requested_workload_seconds": args.duration,
              "rss_samples": [], "rss_sample_count": 0, "errors": [], "restore": {"status": "not_run"},
              "unknown_outcome_policy": "one pending list transition allows pre/post state, never retries remove-on-pop; native enqueue retries use persisted idempotency token",
              "memory_bound": {"list_items_per_worker": args.list_bound, "workers": args.workers,
                               "live_native_jobs_per_worker": 1, "rss_report_samples_max": args.max_rss_samples,
                               "terminal_metadata": "queue deleted after each terminal idempotency check"}}
    started = time.monotonic()
    threads = []
    stop, crashing = threading.Event(), threading.Event()
    rss_count = 0
    first_rss = last_rss = maximum_rss = None

    def sample(label):
        nonlocal rss_count, first_rss, last_rss, maximum_rss
        rss_count += 1
        row = {"elapsed_seconds": time.monotonic() - started, "phase": label,
               "pid": server.process.pid, **current_rss(server.process.pid)}
        if row["bytes"] is not None:
            first_rss = row["bytes"] if first_rss is None else first_rss
            last_rss = row["bytes"]
            maximum_rss = max(maximum_rss or 0, last_rss)
        # Retain a rolling bounded window; aggregate first/last/max across every sample.
        if len(report["rss_samples"]) == args.max_rss_samples:
            report["rss_samples"].pop(0)
        report["rss_samples"].append(row)

    def join_threads():
        deadline = time.monotonic() + args.timeout + 5
        for thread in threads:
            thread.join(max(0, deadline - time.monotonic()))
        alive = [thread.name for thread in threads if thread.is_alive()]
        if alive:
            raise TimeoutError("workload threads failed to stop: " + repr(alive))
        threads.clear()

    try:
        server.start()
        report["command_line"] = server.command_line
        sample("idle")
        workload_started = time.monotonic()
        active_seconds = 0.0
        while active_seconds < args.duration:
            stop, crashing = threading.Event(), threading.Event()
            threads = [threading.Thread(target=worker.run, args=(server.address, stop, crashing),
                                        name=f"worker-{worker.index}") for worker in workers]
            threads += [threading.Thread(target=observer, args=(server.address, workers, args, counters, stop, crashing, connections), name="observer"),
                        threading.Thread(target=maintenance, args=(server.address, args, counters, stop, crashing, connections, storage), name="maintenance")]
            for thread in threads:
                thread.start()
            phase_started = time.monotonic()
            phase_seconds = min(args.duration - active_seconds, args.crash_every) if args.crash_every else args.duration - active_seconds
            phase_end = phase_started + phase_seconds
            next_sample = time.monotonic()
            while time.monotonic() < phase_end and not stop.is_set():
                if time.monotonic() >= next_sample:
                    sample("concurrent_maintenance")
                    next_sample = time.monotonic() + args.rss_interval
                stop.wait(min(.05, max(0, phase_end - time.monotonic())))
            active_seconds += time.monotonic() - phase_started
            if counters.values["failures"]:
                raise AssertionError("concurrent workload failed")
            if args.crash_every:
                crashing.set()
                server.close(crash=True)
                stop.set()
                connections.interrupt()
                join_threads()
                counters.add("forced_process_crashes")
                server.start()
            else:
                stop.set()
                join_threads()
            with Client(server.address, args.timeout) as client:
                for worker in workers:
                    worker.recover(client)
            sample("recovered" if args.crash_every else "drained_workers")
        report["workload_and_recovery_seconds"] = time.monotonic() - workload_started
        report["observed_active_workload_seconds"] = active_seconds
        with Client(server.address, args.timeout) as client:
            client.command("FFKV.CHECKPOINT")
            counters.add("checkpoints")
            if storage == "hybrid":
                client.command("FFKV.COMPACT")
                counters.add("compactions")
            if not args.skip_restore:
                client.command("FFKV.BACKUP", "soak-final")
                counters.add("backups")
        if not args.skip_restore:
            target = Path(directory) / "restored"
            invocation = [binary, "restore", "--source", str(Path(directory) / "data" / "backups" / "soak-final"),
                          "--data-dir", str(target)]
            result = subprocess.run(invocation, capture_output=True, text=True, timeout=args.restore_timeout)
            report["restore"] = {"command": invocation, "exit_code": result.returncode,
                                 "stdout": result.stdout[-8192:], "stderr": result.stderr[-8192:]}
            if result.returncode:
                raise RuntimeError("offline restore failed")
            # OwnedServer normally uses DIRECTORY/data; pass the restored data-dir explicitly.
            restored = OwnedServer(binary, Path(directory) / "restore-process", mode="memory", storage=storage,
                                   timeout=args.timeout, extra_args=["--data-dir", str(target), "--durability", "strict", *extra])
            with restored:
                with Client(restored.address, args.timeout) as client:
                    for worker in workers:
                        worker.recover(client)
                report["restore"]["status"] = "verified_real_process_reopen"
                counters.add("restored_worker_checks", len(workers))
        else:
            report["restore"] = {"status": "explicitly_skipped"}
        sample("final")
    except Exception as error:
        report["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        stop.set()
        connections.interrupt()
        try:
            join_threads()
        except Exception as error:
            report["errors"].append(str(error))
        try:
            server.close()
        except Exception as error:
            report["errors"].append(str(error))
    report["observed_total_seconds"] = time.monotonic() - started
    report["counters"] = dict(counters.values)
    report["errors"].extend(counters.errors)
    report["rss_sample_count"] = rss_count
    report["rss_summary"] = {"first_bytes": first_rss, "last_bytes": last_rss,
                             "maximum_sampled_current_bytes": maximum_rss,
                             "growth_bytes": last_rss - first_rss if first_rss is not None and last_rss is not None else None,
                             "note": "current RSS samples across PID generations; not allocator or cache size"}
    report["ok"] = not report["errors"]
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", default="target/release/ffkv")
    parser.add_argument("--duration", type=float, default=12)
    parser.add_argument("--storage", choices=("memory", "hybrid"), nargs="+", default=["memory", "hybrid"])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--shards", type=int, default=4)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--list-bound", type=int, default=16)
    parser.add_argument("--cache-bytes", type=int, default=32768)
    parser.add_argument("--segment-bytes", type=int, default=1048576)
    parser.add_argument("--crash-every", type=float, default=4, help="0 explicitly disables forced crashes")
    parser.add_argument("--maintenance-interval", type=float, default=1)
    parser.add_argument("--rss-interval", type=float, default=1)
    parser.add_argument("--max-rss-samples", type=int, default=2048)
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--restore-timeout", type=float, default=120)
    parser.add_argument("--skip-restore", action="store_true")
    parser.add_argument("--json", default="-")
    args = parser.parse_args()
    if not __debug__:
        parser.error("run without -O: workload invariants require Python assertions")
    for name in ("duration", "maintenance_interval", "rss_interval", "timeout", "restore_timeout"):
        if not math.isfinite(getattr(args, name)) or getattr(args, name) <= 0:
            parser.error(name + " must be finite and positive")
    if (not math.isfinite(args.crash_every) or args.crash_every < 0 or not 1 <= args.workers <= 64
            or not 1 <= args.shards <= 64 or not 1 <= args.list_bound <= 1024
            or not 1 <= args.payload_bytes <= 1024 * 1024 or not 1 <= args.max_rss_samples <= 100000
            or args.cache_bytes < 0 or args.segment_bytes <= 0
            or args.workers * args.list_bound * args.payload_bytes > 128 * 1024 * 1024):
        parser.error("invalid bounds; expected harness list bodies must fit 128 MiB")
    binary = str(Path(args.binary).resolve()) if Path(args.binary).is_file() else shutil.which(args.binary)
    report = {"schema": 1, "environment": environment(), "settings": vars(args),
              "invocation": sys.argv, "binary_version": version(args.binary), "runs": [], "errors": [],
              "evidence_scope": "observed finite run only; no seven-day, power-loss, HA, or Redis exactly-once claim"}
    if not binary:
        report["errors"].append("ffkv binary unavailable; build separately and supply --binary")
    else:
        with tempfile.TemporaryDirectory(prefix="ffkv-soak-") as directory:
            for storage in args.storage:
                report["runs"].append(run_mode(args, binary, Path(directory) / storage, storage))
    report["ok"] = not report["errors"] and all(run["ok"] for run in report["runs"])
    write_report(report, args.json)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
