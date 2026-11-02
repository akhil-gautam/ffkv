#!/usr/bin/env python3
"""Owned ffkv/Redis list benchmarks; external endpoints are opt-in and never reconfigured.

python3 benchmarks/compare.py --binary target/release/ffkv --redis-binary redis-server \
    --json results.json --csv results.csv
Use --items 1m|5m|10m for larger backlogs (per case, not per worker).
Cold means first application read pass, NOT a flushed OS page cache.
"""
import argparse
import concurrent.futures
import csv
import math
from pathlib import Path
import shutil
import sys
import tempfile
import time
import uuid

from runtime import (Client, Histogram, OwnedServer, ResponseError, current_rss,
                     environment, info_fields, jsonable, version, write_report)


def items(value):
    multiplier = 1000000 if value.lower().endswith("m") else 1
    number = int(value[:-1] if multiplier != 1 else value) * multiplier
    if number < 1:
        raise argparse.ArgumentTypeError("items must be positive")
    return number


def external(value):
    try:
        kind, address = value.split("=", 1)
        host, port = address.rsplit(":", 1)
        if kind not in ("redis", "valkey", "dragonfly") or not host or not 0 < int(port) < 65536:
            raise ValueError()
        return kind, (host, int(port))
    except ValueError:
        raise argparse.ArgumentTypeError("expected redis|valkey|dragonfly=HOST:PORT") from None


def persistence(client, mode):
    result = {"requested": mode, "status": "unverified", "settings": {}, "errors": []}
    expected = {"appendonly": "no" if mode == "memory" else "yes", "save": ""}
    if mode != "memory":
        expected.update(appendfsync="always" if mode == "strict" else "everysec",
                        **{"no-appendfsync-on-rewrite": "no"})
    for setting in expected:
        try:
            row = client.command("CONFIG", "GET", setting)
            if not isinstance(row, list) or len(row) != 2:
                raise ValueError("setting not returned")
            result["settings"][setting] = row[1].decode()
        except (ResponseError, ValueError, UnicodeError) as error:
            result["errors"].append(f"{setting}: {error}")
    result["expected"] = expected
    if not result["errors"]:
        result["status"] = "verified" if result["settings"] == expected else "mismatch"
    return result


def execute_phase(address, timeout, counts, pipeline, command, expected, duration=0):
    started = time.monotonic()
    deadline = started + duration if duration else None

    def worker(worker_id):
        stats = {"attempted": 0, "completed": 0, "errors": 0, "unknown_outcomes": 0,
                 "retries": 0, "error_examples": []}
        histogram = Histogram()
        count, index = counts[worker_id], 0
        try:
            with Client(address, timeout) as client:
                while index < count or (deadline is not None and time.monotonic() < deadline):
                    size = min(pipeline, count - index) if deadline is None else pipeline
                    if size <= 0:
                        break
                    frames = [Client.frame(command(worker_id, (index + k) % max(1, count)))
                              for k in range(size)]
                    begin = time.perf_counter()
                    stats["attempted"] += size
                    pending = size
                    try:
                        client.socket.sendall(b"".join(frames))
                        for k in range(size):
                            reply = client.read()
                            pending -= 1
                            histogram.add(time.perf_counter() - begin)
                            if isinstance(reply, ResponseError):
                                raise reply
                            if not expected(reply, worker_id, (index + k) % max(1, count)):
                                raise AssertionError(f"unexpected reply type/value: {repr(reply)[:160]}")
                            stats["completed"] += 1
                    except BaseException:
                        stats["unknown_outcomes"] += pending
                        raise
                    index += size
        except Exception as error:
            stats["errors"] += 1
            stats["error_examples"].append(f"worker {worker_id}: {type(error).__name__}: {error}")
        return stats, histogram

    total = {key: 0 for key in ("attempted", "completed", "errors", "unknown_outcomes", "retries")}
    histogram = Histogram()
    examples = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(counts)) as pool:
        for stats, samples in pool.map(worker, range(len(counts))):
            for key in total:
                total[key] += stats[key]
            histogram.merge(samples)
            examples.extend(stats["error_examples"])
    elapsed = time.monotonic() - started
    return {**total, **histogram.report(), "elapsed_seconds": elapsed,
            "completed_ops_per_second": total["completed"] / elapsed,
            "error_examples": examples,
            "latency_semantics": "send-start to each reply; includes pipeline queueing; no inferred per-command division"}


def run_case(args, kind, mode, storage, payload_size, server=None, address=None):
    address = address or server.address
    pid = server.process.pid if server else None
    prefix = f"ffkv:benchmark:{uuid.uuid4().hex}:"
    keys = [(prefix + str(i)).encode() for i in range(args.concurrency)]
    counts = [args.items // args.concurrency + (i < args.items % args.concurrency)
              for i in range(args.concurrency)]
    payload = (bytes(range(256)) * math.ceil(payload_size / 256))[:payload_size]
    result = {"engine": kind, "storage": storage, "durability": mode,
              "payload_bytes": payload_size, "items": args.items,
              "concurrency": args.concurrency, "pipeline": args.pipeline,
              "socket_timeout_seconds": args.timeout, "address": list(address),
              "ownership": "owned child/tempdir" if server else "explicit external; only unique namespace mutated",
              "command_line": server.command_line if server else None,
              "phases": [], "rss": [], "errors": [], "cleanup_errors": [],
              "cold_definition": "first LINDEX pass over backlog; OS cache NOT evicted",
              "semantics": "LPUSH/LINDEX/LPOP Redis-compatible lists, remove-on-pop; no native ACK comparison"}

    def rss(phase):
        sample = current_rss(pid) if pid else {"bytes": None, "source": "unavailable for external endpoint"}
        result["rss"].append({"phase": phase, "monotonic_seconds": time.monotonic(), **sample})

    def phase(name, sizes, build, valid, duration=0):
        row = execute_phase(address, args.timeout, sizes, args.pipeline, build, valid, duration)
        result["phases"].append({"phase": name, **row})
        if row["errors"] or row["unknown_outcomes"]:
            raise RuntimeError(f"{name} failed; see phase errors")

    try:
        with Client(address, args.timeout) as client:
            if kind == "ffkv":
                result["server_identity"] = jsonable(client.command("FFKV.STATS"))
                result["persistence_verification"] = {
                    "status": "owned exact CLI settings", "mode": mode, "storage": storage}
            else:
                result["server_identity"] = info_fields(client.command("INFO", "server"))
                identity = result["server_identity"]
                detected = ("valkey" if "valkey_version" in identity else
                            "dragonfly" if "dragonfly_version" in identity else
                            "redis-compatible" if "redis_version" in identity else "unknown")
                result["engine_identity_verification"] = {
                    "requested": kind, "detected_signature": detected,
                    "status": "verified" if detected == kind else "unverified",
                    "note": "redis_version alone does not distinguish all compatible servers"}
                result["persistence_verification"] = persistence(client, mode)
                if server and result["persistence_verification"]["status"] != "verified":
                    raise RuntimeError("owned Redis persistence settings did not verify")
            rss("idle")
            phase("backlog_enqueue", counts, lambda w, _: ("LPUSH", keys[w], payload),
                  lambda value, w, _: isinstance(value, int) and 0 < value <= counts[w])
            for worker, count in enumerate(counts):
                if client.command("LLEN", keys[worker]) != count:
                    raise AssertionError("backlog length mismatch")
            rss("backlog")
            phase("cold_first_read", counts, lambda w, i: ("LINDEX", keys[w], i),
                  lambda value, *_: value == payload)
            phase("hot_repeat_read", [args.hot_operations for _ in counts],
                  lambda w, i: ("LINDEX", keys[w], i % min(counts[w], args.hot_items)),
                  lambda value, *_: value == payload, args.duration)
            partial = [n // 2 for n in counts]
            phase("half_drain", partial, lambda w, _: ("LPOP", keys[w]), lambda value, *_: value == payload)
            rss("half_drain")
            begin = time.monotonic()
            maintenance = {"phase": "compaction", "errors": 0, "status": "not_applicable"}
            if kind == "ffkv":
                if storage == "hybrid":
                    maintenance["response"] = jsonable(client.command("FFKV.COMPACT"))
                    maintenance["status"] = "completed"
                else:
                    maintenance["reason"] = "memory payload storage has no blob compaction"
                if mode != "memory":
                    checkpoint = time.monotonic()
                    maintenance["checkpoint_response"] = jsonable(client.command("FFKV.CHECKPOINT"))
                    maintenance["checkpoint_seconds"] = time.monotonic() - checkpoint
                    maintenance["status"] = "checkpoint_completed"
            elif mode != "memory":
                # AOF rewrite is measured separately, never equated with blob compaction.
                maintenance["operation"] = "BGREWRITEAOF (not equivalent to ffkv blob compaction)"
                maintenance["response"] = jsonable(client.command("BGREWRITEAOF"))
                deadline = time.monotonic() + args.maintenance_timeout
                while True:
                    info = info_fields(client.command("INFO", "persistence"))
                    if info.get("aof_rewrite_in_progress") == "0" and info.get("aof_rewrite_scheduled", "0") == "0":
                        if info.get("aof_last_bgrewrite_status") != "ok":
                            raise RuntimeError("AOF rewrite failed")
                        break
                    if time.monotonic() >= deadline:
                        raise TimeoutError("AOF rewrite did not finish")
                    time.sleep(.05)
                maintenance["status"] = "completed"
            else:
                maintenance["reason"] = "memory-only Redis has no payload-store compaction"
            maintenance["elapsed_seconds"] = time.monotonic() - begin
            result["phases"].append(maintenance)
            rss("post_compaction")
            phase("final_drain", [n - p for n, p in zip(counts, partial)],
                  lambda w, _: ("LPOP", keys[w]), lambda value, *_: value == payload)
            for key in keys:
                if client.command("LLEN", key) != 0:
                    raise AssertionError("drained list is not empty")
            rss("drain")
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    finally:
        try:
            with Client(address, args.timeout) as client:
                for key in keys:
                    client.command("DEL", key)
        except Exception as error:
            result["cleanup_errors"].append(str(error))
    result["ok"] = not result["errors"] and not result["cleanup_errors"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", default="target/release/ffkv")
    parser.add_argument("--redis-binary", default="redis-server")
    parser.add_argument("--skip-redis", action="store_true", help="explicitly omit owned Redis; no results invented")
    parser.add_argument("--external", action="append", type=external, default=[])
    parser.add_argument("--external-durability", choices=("memory", "strict", "everysec"), default="strict")
    parser.add_argument("--modes", nargs="+", choices=("memory", "strict", "everysec"), default=["memory", "strict", "everysec"])
    parser.add_argument("--payloads", type=int, nargs="+", default=[256, 4096, 65536])
    parser.add_argument("--items", type=items, default=512)
    parser.add_argument("--concurrency", "--jobs", type=int, default=4)
    parser.add_argument("--pipeline", type=int, default=8)
    parser.add_argument("--hot-items", type=int, default=4)
    parser.add_argument("--hot-operations", type=int, default=256, help="hot reads per worker when --duration is 0")
    parser.add_argument("--duration", type=float, default=0, help="hot-phase seconds per case; 0 uses --hot-operations")
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("--maintenance-timeout", type=float, default=120)
    parser.add_argument("--inline-threshold", type=int, default=1024)
    parser.add_argument("--cache-bytes", type=int, default=1048576)
    parser.add_argument("--segment-bytes", type=int, default=8388608)
    parser.add_argument("--disk-budget", type=int, default=0)
    parser.add_argument("--json", default="-")
    parser.add_argument("--csv")
    args = parser.parse_args()
    if (not 1 <= args.concurrency <= min(args.items, 128) or not 1 <= args.pipeline <= 128
            or args.pipeline * (max(args.payloads, default=0) + 256) > 8 * 1024 * 1024
            or not args.payloads or min(args.payloads) < 1 or max(args.payloads) > 1024 * 1024
            or args.hot_items < 1 or args.hot_operations < 1 or args.duration < 0 or not math.isfinite(args.duration)
            or args.timeout <= 0 or not math.isfinite(args.timeout)
            or args.maintenance_timeout <= 0 or not math.isfinite(args.maintenance_timeout)
            or min(args.inline_threshold, args.cache_bytes, args.disk_budget) < 0 or args.segment_bytes <= 0):
        parser.error("invalid concurrency/pipeline/payload/duration/timeout/storage bounds")
    report = {"schema": 1, "environment": environment(), "settings": vars(args).copy(),
              "versions": {"ffkv": version(args.binary)}, "cases": [], "errors": [],
              "claims": "measured workload only; no absent engines tested; no advantage or long-duration claim",
              "invocation": sys.argv}
    ffkv = str(Path(args.binary).resolve()) if Path(args.binary).exists() else shutil.which(args.binary)
    redis = shutil.which(args.redis_binary)
    if not ffkv:
        report["errors"].append("ffkv binary unavailable; build separately and supply --binary")
    if not args.skip_redis and not redis:
        report["errors"].append("redis-server unavailable; install/supply --redis-binary or explicitly --skip-redis")
    if redis and not args.skip_redis:
        report["versions"]["redis"] = version(redis)
    with tempfile.TemporaryDirectory(prefix="ffkv-benchmark-") as directory:
        for mode in args.modes:
            targets = []
            if ffkv:
                targets += [("ffkv", ffkv, "memory")]
                if mode != "memory":
                    targets += [("ffkv", ffkv, "hybrid")]
            if redis and not args.skip_redis:
                targets += [("redis", redis, "memory")]
            for kind, binary, storage in targets:
                for payload in args.payloads:
                    data = Path(directory) / f"{kind}-{mode}-{storage}-{payload}"
                    extra = ["--inline-threshold", str(args.inline_threshold), "--cache-bytes", str(args.cache_bytes),
                             "--segment-bytes", str(args.segment_bytes), "--disk-budget", str(args.disk_budget)] if storage == "hybrid" else []
                    try:
                        with OwnedServer(binary, data, kind, mode, storage, args.timeout, extra) as server:
                            report["cases"].append(run_case(args, kind, mode, storage, payload, server=server))
                    except Exception as error:
                        report["errors"].append(f"{kind}/{mode}/{storage}/{payload}: {type(error).__name__}: {error}")
        for kind, address in args.external:
            for payload in args.payloads:
                report["cases"].append(run_case(args, kind, args.external_durability,
                                                 "external/unverified", payload, address=address))
    report["ok"] = not report["errors"] and all(case["ok"] for case in report["cases"])
    write_report(report, args.json)
    if args.csv:
        columns = ["engine", "storage", "durability", "payload_bytes", "items", "concurrency", "pipeline",
                   "phase", "completed", "attempted", "errors", "unknown_outcomes", "retries", "elapsed_seconds",
                   "completed_ops_per_second", "p50_ms", "p95_ms", "p99_ms", "max_ms", "percentile_method",
                   "latency_semantics", "status", "case_ok", "case_errors_json", "cleanup_errors_json",
                   "run_errors_json", "settings_json", "rss_json"]
        with open(args.csv, "w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            import json
            if report["errors"]:
                writer.writerow({"phase": "run_error", "case_ok": False, "run_errors_json": json.dumps(report["errors"]),
                                 "settings_json": json.dumps(jsonable({"settings": report["settings"],
                                     "environment": report["environment"], "versions": report["versions"]}))})
            for case in report["cases"]:
                base = {k: case.get(k) for k in columns if k in case}
                for phase in case["phases"] or [{"phase": "case_failed_before_workload"}]:
                    writer.writerow({**base, **phase, "case_ok": case["ok"],
                        "case_errors_json": json.dumps(case["errors"]),
                        "cleanup_errors_json": json.dumps(case["cleanup_errors"]),
                        "run_errors_json": json.dumps(report["errors"]), "settings_json": json.dumps(jsonable({
                        "settings": report["settings"], "environment": report["environment"],
                        "versions": report["versions"], "persistence": case.get("persistence_verification"),
                        "server_identity": case.get("server_identity"), "command_line": case["command_line"]})),
                        "rss_json": json.dumps(case["rss"])})
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
