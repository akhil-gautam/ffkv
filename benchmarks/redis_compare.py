#!/usr/bin/env python3
"""Isolated ffkv/Redis/Dragonfly comparison using the compiled redis-benchmark client.

All endpoints are fresh owned children. Warmup and calibration are excluded from
summary statistics. Durable runs measure acknowledged writes, not crash recovery.
Engine/mode pairs without an equivalent local durability mechanism are recorded as
explicit unsupported trials: never executed, never passes, never a win.
Exit status: 0 all requested trials passed, 1 any failure, 3 no failures but some
requested trials were unsupported.
"""
import argparse
import contextlib
import csv
import io
import math
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

from runtime import (ENGINE_KINDS, ENGINE_MODES, UNSUPPORTED, Client, OwnedServer, ResponseError,
                     affinity_error, affinity_prefix, cpu_list_argument, current_rss,
                     dragonfly_args, dragonfly_settings, environment, info_fields, jsonable,
                     linux_observations, mapping, version, write_report)


PROFILES = {
    "c1-p1-t1": (1, 1, 1),
    "c32-p1-t4": (32, 1, 4),
    "c32-p16-t4": (32, 16, 4),
    "c32-p1-t1": (32, 1, 1),
    "c32-p1-t8": (32, 1, 8),
    "c32-p16-t1": (32, 16, 1),
    "c32-p16-t8": (32, 16, 8),
}
WORKLOADS = {
    "get-256": ("GET", "string", 10000, 256),
    "set-256": ("SET", "string", 10000, 256),
    "get-4096": ("GET", "string", 10000, 4096),
    "set-4096": ("SET", "string", 10000, 4096),
    "incr": ("INCR", "counter", 10000, 0),
    "hget": ("HGET", "hash", 1000, 256),
    "hset": ("HSET", "hash", 1000, 256),
    "zadd": ("ZADD", "zset", 1000, 0),
    "zrange": ("ZRANGE", "zset", 1000, 0),
}
LATENCY_SEMANTICS = (
    "Unmodified redis-benchmark CSV latency in milliseconds. Redis 8.2.1 records "
    "send-to-first-socket-read-event latency before parsing replies, reusing that "
    "sample for every reply in a pipeline. This is neither batch-completion "
    "latency nor individual-reply latency, and is never divided by pipeline depth. "
    "The native histogram clamps samples at 3000000 microseconds (3 seconds). "
    "Only fields actually emitted by this binary are reported; these source-level "
    "semantics were established for Redis 8.2.1, not arbitrary client versions."
)
# Redis 8.2.1 source-level semantics above are only claimed when the client reports it.
NATIVE_CLIENT_VERSION = "8.2.1"


def host_environment():
    result = environment()
    result["client"] = "compiled redis-benchmark; stdlib RESP only for setup and validation"
    result["limitations"] = []
    if platform.system() == "Darwin":
        result["macos_observations"] = []
        commands = [
            ["/usr/sbin/sysctl", "machdep.cpu.brand_string", "hw.memsize"],
            ["/usr/bin/pmset", "-g"],
            ["/usr/bin/pmset", "-g", "batt"],
        ]
        for command in commands:
            observation = {"command": command}
            try:
                completed = subprocess.run(command, capture_output=True, text=True, timeout=5)
                observation.update(stdout=completed.stdout, stderr=completed.stderr,
                                   exit_code=completed.returncode)
                if re.search(r"lowpowermode\s+1\b", completed.stdout):
                    result["limitations"].append("Low Power Mode is enabled; not representative of unrestricted peak performance.")
                if "Now drawing from 'Battery Power'" in completed.stdout:
                    result["limitations"].append("Host is battery powered; power settings were not changed.")
            except (OSError, subprocess.SubprocessError) as error:
                observation["error"] = str(error)
            result["macos_observations"].append(observation)
    if platform.system() == "Linux":
        result["linux_observations"] = linux_observations()
    return result


def key(index):
    # redis-benchmark substitutes a zero-padded 12-digit integer in [0, -r).
    return f"native-bench:{index:012d}"


def payload(size):
    return "x" * size


def workload_command(name):
    operation, _, _, size = WORKLOADS[name]
    template = "native-bench:__rand_int__"
    if operation == "SET":
        return [operation, template, payload(size - 12) + "__rand_int__"]
    if operation in ("HGET", "HSET"):
        return [operation, template, "field0"] + ([payload(size - 12) + "__rand_int__"] if operation == "HSET" else [])
    if operation == "ZADD":
        return [operation, template, "__rand_int__", "member00"]
    if operation == "ZRANGE":
        return [operation, template, "0", "9"]
    return [operation, template]


def checked_pipeline(client, commands, expected, batch_size=64, deadline=None):
    """Bound queued commands, bytes and total wall time; validate every reply."""
    batch = []
    original_timeout = client.socket.gettimeout()

    def remaining_timeout():
        if deadline is None:
            return original_timeout
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("bounded RESP phase deadline exceeded")
        return min(original_timeout, remaining) if original_timeout else remaining

    def flush():
        client.socket.settimeout(remaining_timeout())
        client.socket.sendall(b"".join(Client.frame(command) for command in batch))
        replies = []
        for command in batch:
            client.socket.settimeout(remaining_timeout())
            reply = client.read()
            if isinstance(reply, ResponseError):
                raise reply
            if not expected(reply):
                raise AssertionError(f"unexpected {command[0]} reply: {repr(reply)[:200]}")
            replies.append(reply)
        return replies

    try:
        for command in commands:
            batch.append(command)
            if len(batch) == batch_size:
                yield from flush()
                batch.clear()
        if batch:
            yield from flush()
    finally:
        client.socket.settimeout(original_timeout)


def preload(client, name, timeout):
    _, kind, count, size = WORKLOADS[name]

    def commands():
        for index in range(count):
            if kind in ("string", "counter"):
                yield ["SET", key(index), "0" if kind == "counter" else payload(size)]
            elif kind == "hash":
                yield ["HSET", key(index), *[item for field in range(8)
                                            for item in (f"field{field}", payload(size))]]
            else:
                yield ["ZADD", key(index), *[item for member in range(64)
                                            for item in (str(member), f"member{member:02d}")]]

    expected = b"OK" if kind in ("string", "counter") else 8 if kind == "hash" else 64
    started = time.monotonic()
    validated = sum(1 for _ in checked_pipeline(
        client, commands(), lambda reply: reply == expected,
        deadline=started + timeout))
    if validated != count:
        raise AssertionError("incomplete preload")
    return {"commands": validated, "all_replies_validated": True,
            "pipeline_bound": 64, "elapsed_seconds": time.monotonic() - started,
            "key_count": count, "fields_per_hash": 8 if kind == "hash" else None,
            "members_per_sorted_set": 64 if kind == "zset" else None}


def valid_payload(value, size, count, changing):
    if value == payload(size).encode():
        return True  # A preloaded key may not have been selected by the random writer.
    return (changing and isinstance(value, bytes) and len(value) == size
            and value[:-12] == payload(size - 12).encode()
            and value[-12:].isdigit() and 0 <= int(value[-12:]) < count)


def validate(client, name, timeout):
    operation, kind, count, size = WORKLOADS[name]
    indices = sorted({0, 1, count - 1, *(i * (count - 1) // 16 for i in range(17))})
    result = {"ok": False, "sample_indices": indices, "samples": [], "errors": []}
    try:
        actual_count = client.command("DBSIZE")
        result["database_key_count"] = actual_count
        if actual_count != count:
            raise AssertionError(f"DBSIZE {actual_count}, expected {count}")
        for index in indices:
            name_key = key(index)
            actual_type = client.command("TYPE", name_key)
            expected_type = b"string" if kind == "counter" else kind.encode()
            if actual_type != expected_type:
                raise AssertionError(f"TYPE {name_key}: {actual_type!r} != {expected_type!r}")
            sample = {"key": name_key, "type": actual_type.decode()}
            result["samples"].append(sample)
            if kind == "string":
                value = client.command("GET", name_key)
                if not valid_payload(value, size, count, operation == "SET"):
                    raise AssertionError(f"GET {name_key}: missing or incorrect payload")
                sample["value_bytes"] = len(value)
            elif kind == "counter":
                value = client.command("GET", name_key)
                if not isinstance(value, bytes) or not value.isdigit():
                    raise AssertionError(f"GET {name_key}: invalid counter {value!r}")
                sample["counter"] = int(value)
            elif kind == "hash":
                length = client.command("HLEN", name_key)
                if length != 8:
                    raise AssertionError(f"HLEN {name_key}: {length} != 8")
                for field in range(8):
                    if not valid_payload(client.command("HGET", name_key, f"field{field}"),
                                         size, count, operation == "HSET" and field == 0):
                        raise AssertionError(f"HGET {name_key} field{field}: incorrect payload")
                sample["fields"] = length
                sample["all_field_values_validated"] = True
            else:
                length = client.command("ZCARD", name_key)
                if length != 64:
                    raise AssertionError(f"ZCARD {name_key}: {length} != 64")
                score = client.command("ZSCORE", name_key, "member00")
                if score is None or not math.isfinite(float(score)) or not 0 <= float(score) < count:
                    raise AssertionError(f"ZSCORE {name_key}: incorrect member00 score")
                if operation == "ZRANGE" and float(score) != 0:
                    raise AssertionError(f"ZRANGE workload mutated {name_key}")
                ordered = sorted([(float(score), b"member00")]
                                 + [(float(i), f"member{i:02d}".encode()) for i in range(1, 64)])
                members = client.command("ZRANGE", name_key, 0, 9)
                if members != [member for _, member in ordered[:10]]:
                    raise AssertionError(f"ZRANGE {name_key}: incorrect top-10 members")
                sample.update(members=length, top_10=jsonable(members), member00_score=float(score))
        if kind == "counter":
            # Outside timing: the full sum also detects missing or excess INCRs.
            values = checked_pipeline(client, (["GET", key(i)] for i in range(count)),
                                      lambda value: isinstance(value, bytes) and value.isdigit(),
                                      deadline=time.monotonic() + timeout)
            result["counter_sum"] = sum(int(value) for value in values)
            result["all_counters_validated"] = count
        result["ok"] = True
    except Exception as error:
        result["errors"].append(f"{type(error).__name__}: {error}")
    return result


def persistence(client, mode):
    expected = {"appendonly": "no" if mode == "memory" else "yes", "save": "",
                "appendfsync": "always" if mode == "strict" else "everysec",
                "no-appendfsync-on-rewrite": "no", "auto-aof-rewrite-percentage": "0"}
    result = {"expected": expected, "actual": {}, "ok": False}
    for setting in expected:
        reply = client.command("CONFIG", "GET", setting)
        if not isinstance(reply, list) or len(reply) != 2 or reply[0] != setting.encode():
            raise AssertionError(f"CONFIG GET {setting}: {reply!r}")
        result["actual"][setting] = reply[1].decode()
    result["ok"] = result["actual"] == expected
    return result


def process_threads(pid):
    """Linux thread names of the owned server, outside timed execution."""
    if platform.system() != "Linux":
        return {"source": "unavailable off Linux"}
    names = {}
    try:
        for task in Path(f"/proc/{pid}/task").iterdir():
            with contextlib.suppress(OSError):
                name = (task / "comm").read_text().strip()
                names[name] = names.get(name, 0) + 1
    except OSError as error:
        return {"source": "unavailable", "error": str(error)}
    return {"source": "/proc/PID/task/*/comm", "count": sum(names.values()), "names": names}


def server_stats(client, engine):
    if engine == "ffkv":
        raw = client.command("FFKV.STATS")
        fields = mapping(raw)
        return {"command": ["FFKV.STATS"], "response": jsonable(raw),
                "server": jsonable(mapping(fields[b"server"])),
                "wal": jsonable(mapping(fields[b"wal"])), "key_count": fields[b"key_count"]}
    raw = client.command("INFO", "ALL")
    return {"command": ["INFO", "ALL"], "raw": raw.decode(errors="replace"), "fields": info_fields(raw)}


def process_cpu(pid):
    """Cumulative server CPU, sampled only outside timed native execution."""
    try:
        if platform.system() == "Linux":
            stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            hz = os.sysconf("SC_CLK_TCK")
            return {"seconds": (int(stat[11]) + int(stat[12])) / hz,
                    "source": "/proc/PID/stat utime+stime", "resolution_seconds": 1 / hz}
        command = ["ps", "-o", "time=", "-p", str(pid)]
        completed = subprocess.run(command, capture_output=True, text=True, timeout=3, check=True)
        text = completed.stdout.strip()
        days, clock = text.split("-", 1) if "-" in text else ("0", text)
        seconds = 0.0
        for part in clock.split(":"):
            seconds = seconds * 60 + float(part)
        return {"seconds": int(days) * 86400 + seconds, "source": "ps cumulative user+system CPU",
                "raw": text, "command": command, "resolution_seconds": 0.01}
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return {"seconds": None, "source": "unavailable", "error": str(error)}


def native_csv(stdout):
    """Preserve native names; support modern headers and old throughput-only CSV."""
    header = None
    rows = []
    for row in csv.reader(io.StringIO(stdout)):
        if len(row) < 2:
            continue
        normalized = [field.strip().lower() for field in row]
        if "rps" in normalized or "requests_per_second" in normalized:
            header = row
            continue
        try:
            throughput = float(row[1])
        except ValueError:
            continue
        if not math.isfinite(throughput) or throughput <= 0:
            raise ValueError("native throughput must be finite and positive")
        if header is not None:
            if len(row) != len(header):
                raise ValueError("native CSV header/value length mismatch")
            fields = dict(zip(header, row))
        elif len(row) == 2:
            fields = {"test": row[0], "rps": row[1]}
        else:
            raise ValueError("native CSV has latency columns but no recognizable header")
        rows.append({"requests_per_second": throughput, "native_fields": fields})
    if len(rows) != 1:
        raise ValueError(f"expected one native CSV result, got {len(rows)}")
    return rows[0]


def run_native(args, server, name, profile, count, phase):
    clients, pipeline, threads = PROFILES[profile]
    command = [*affinity_prefix(args.client_cpus),
               args.benchmark_binary, "-h", server.address[0], "-p", str(server.address[1]),
               "-c", str(clients), "-P", str(pipeline), "--threads", str(threads),
               "-n", str(count), "-r", str(WORKLOADS[name][2]), "--seed", str(args.seed),
               "--csv", *workload_command(name)]
    result = {"phase": phase, "included_in_summary": phase == "measured", "command": command,
              "client_cpus": args.client_cpus,
              "requested_commands": count, "seed": args.seed, "timeout_seconds": args.timeout,
              "stdout": "", "stderr": "", "exit_code": None, "timed_out": False,
              "errors": [], "ok": False, "latency_semantics": LATENCY_SEMANTICS}
    cpu_before = process_cpu(server.process.pid)
    result["server_cpu_before"] = cpu_before
    child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    process = None
    try:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            stdout, stderr = process.communicate(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            result["timed_out"] = True
            process.kill()
            stdout, stderr = process.communicate()
        result.update(stdout=stdout.decode(errors="replace"), stderr=stderr.decode(errors="replace"),
                      exit_code=process.returncode)
    except (Exception, KeyboardInterrupt) as error:
        if process is not None:
            if process.poll() is None:
                process.kill()
            stdout, stderr = process.communicate()
            result.update(stdout=stdout.decode(errors="replace"), stderr=stderr.decode(errors="replace"),
                          exit_code=process.returncode)
        result["errors"].append(f"{type(error).__name__}: {error}")
        result["interrupted"] = isinstance(error, KeyboardInterrupt)
    finally:
        result["wall_seconds"] = time.monotonic() - started
        child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        result["load_generator_cpu"] = {
            "user_seconds": child_after.ru_utime - child_before.ru_utime,
            "system_seconds": child_after.ru_stime - child_before.ru_stime,
            "source": "RUSAGE_CHILDREN delta; only native child reaped in this interval"}
        cpu_after = process_cpu(server.process.pid)
        result["server_cpu_after"] = cpu_after
        result["server_cpu_seconds"] = (cpu_after["seconds"] - cpu_before["seconds"]
                                           if None not in (cpu_after["seconds"], cpu_before["seconds"])
                                           else None)
    if result["timed_out"]:
        result["errors"].append("native load generator timed out and was killed")
    if result["exit_code"] != 0:
        result["errors"].append(f"native exit status {result['exit_code']}")
    # redis-benchmark can print protocol errors even when its exit status is zero.
    diagnostic_lines = (result["stdout"] + "\n" + result["stderr"]).splitlines()
    for line in diagnostic_lines:
        if re.search(r"(^|\s)(?:Error(?:\s+from\s+server)?\s*:|ERR\s|WRONGTYPE\s|OOM\s|LOADING\s)", line, re.I):
            result["errors"].append(line)
    try:
        result["csv"] = native_csv(result["stdout"])
        result["native_duration_seconds_from_count_and_rps"] = count / result["csv"]["requests_per_second"]
    except (ValueError, csv.Error) as error:
        result["errors"].append(f"CSV: {error}")
    result["ok"] = not result["errors"]
    return result


def aligned_count(requested, profile, minimum, maximum):
    clients, pipeline, threads = PROFILES[profile]
    quantum = clients * pipeline  # Also divisible by the number of native threads.
    low = math.ceil(minimum / quantum) * quantum
    high = maximum // quantum * quantum
    if low > high:
        raise ValueError(f"request bounds cannot accommodate profile {profile}")
    return max(low, min(high, math.ceil(requested / quantum) * quantum))


def engine_launch(args, engine):
    """Binary plus engine-specific tuning; every value is recorded with the trial."""
    if engine == "ffkv":
        return args.binary, ("--shards", str(args.ffkv_shards), "--wal-checkpoint-bytes", "0")
    if engine == "dragonfly":
        return args.dragonfly_binary, tuple(dragonfly_args(args.dragonfly_threads))
    return args.redis_binary, ()


def unsupported_trial(engine, mode, name, profile, repetition, order):
    return {"engine": engine, "mode": mode, "storage": "memory", "workload": name,
            "profile": profile, "clients": PROFILES[profile][0], "pipeline": PROFILES[profile][1],
            "threads": PROFILES[profile][2], "repetition": repetition, "engine_order": order,
            "status": "unsupported", "executed": False, "ok": False,
            "unsupported": UNSUPPORTED[(engine, mode)], "phases": [], "correctness": [], "errors": []}


def run_trial(args, engine, mode, name, profile, repetition, order, directory):
    trial = {"engine": engine, "mode": mode, "storage": "memory", "workload": name,
             "profile": profile, "clients": PROFILES[profile][0], "pipeline": PROFILES[profile][1],
             "threads": PROFILES[profile][2], "repetition": repetition, "engine_order": order,
             "executed": True, "phases": [], "correctness": [], "errors": [], "ok": False}
    binary, extra_args = engine_launch(args, engine)
    server = OwnedServer(binary, directory, kind=engine, mode=mode, storage="memory",
                         timeout=args.startup_timeout, extra_args=extra_args, cpus=args.server_cpus)
    trial["launch"] = {"binary": binary, "engine_args": list(extra_args), "server_cpus": args.server_cpus,
                       "client_cpus": args.client_cpus, "directory": str(directory),
                       "ffkv_shards": args.ffkv_shards if engine == "ffkv" else None,
                       "dragonfly_proactor_threads": args.dragonfly_threads if engine == "dragonfly" else None}
    try:
        server.start()
        trial.update(server_command=server.command_line, server_args=server.server_args,
                     address=list(server.address), pid=server.process.pid,
                     server_identity=server.identity, server_affinity=server.affinity)
        with Client(server.address, args.socket_timeout) as client:
            trial["stats_before_preload"] = server_stats(client, engine)
            if engine == "redis":
                trial["persistence"] = persistence(client, mode)
            elif engine == "dragonfly":
                trial["persistence"] = dragonfly_settings(client, server, args.dragonfly_threads)
            else:
                actual = trial["stats_before_preload"]["server"]
                expected = {"shards": args.ffkv_shards, "storage_mode": "memory",
                            "durability": "none" if mode == "memory" else mode}
                trial["persistence"] = {
                    "ok": all(actual.get(setting) == value for setting, value in expected.items()),
                    "source": "FFKV.STATS effective settings and owned CLI; no crash-recovery claim",
                    "expected": expected, "actual": actual, "wal_enabled": mode != "memory"}
            if not trial["persistence"]["ok"]:
                raise AssertionError("effective server settings mismatch")
            trial["server_threads_empty"] = process_threads(server.process.pid)
            trial["rss_empty"] = current_rss(server.process.pid)
            trial["preload"] = preload(client, name, args.preload_timeout)

            def check(label):
                result = validate(client, name, args.preload_timeout)
                trial["correctness"].append({"phase": label, **result})
                if not result["ok"]:
                    raise AssertionError(f"correctness check failed: {label}")
                return result

            check("after_preload")
            def native(phase, count):
                result = run_native(args, server, name, profile, count, phase)
                trial["phases"].append(result)
                if result.get("interrupted"):
                    raise KeyboardInterrupt()
                if not result["ok"]:
                    raise RuntimeError(f"{phase} failed; raw evidence retained")
                return result

            native("warmup", aligned_count(2000, profile, 2000, 5000))
            pilot_count = aligned_count(3000, profile, 2000, 5000)
            calibration_target = min(args.target_seconds, 1.0)
            for iteration in range(args.calibration_iterations):
                pilot = native("calibration", pilot_count)
                pilot["calibration_iteration"] = iteration + 1
                pilot_duration = pilot["native_duration_seconds_from_count_and_rps"]
                if pilot_duration >= calibration_target * 0.8:
                    break
                next_count = aligned_count(
                    max(pilot_count * 2, pilot["csv"]["requests_per_second"] * calibration_target),
                    profile, args.min_requests, args.max_requests)
                if next_count <= pilot_count:
                    break
                pilot_count = next_count
            estimated = pilot["csv"]["requests_per_second"] * args.target_seconds
            count = aligned_count(estimated, profile, args.min_requests, args.max_requests)
            trial["calibration"] = {"target_seconds": args.target_seconds,
                                    "pilot_target_seconds": calibration_target,
                                    "pilot_native_rps": pilot["csv"]["requests_per_second"],
                                    "pilot_target_met": pilot_duration >= calibration_target * 0.8,
                                    "iterations": iteration + 1,
                                    "unbounded_request_estimate": estimated, "measured_requests": count,
                                    "min_requests": args.min_requests, "max_requests": args.max_requests,
                                    "method": "bounded iterative excluded pilots; final native rps times target"}
            before = check("before_measured")
            trial["stats_before_measured"] = server_stats(client, engine)
            trial["rss_before_measured"] = current_rss(server.process.pid)
            measured = run_native(args, server, name, profile, count, "measured")
            trial["phases"].append(measured)
            if measured.get("interrupted"):
                raise KeyboardInterrupt()
            duration = measured.get("native_duration_seconds_from_count_and_rps")
            trial["duration_quality"] = {
                "target_seconds": args.target_seconds, "native_duration_seconds": duration,
                "target_met": duration is not None and duration >= args.target_seconds * 0.8,
                "sustained_at_least_one_second": duration is not None and duration >= 1.0,
                "note": "native duration is requested count / reported rps; not independent timing"}
            trial["rss_after_measured"] = current_rss(server.process.pid)
            trial["server_threads_after_measured"] = process_threads(server.process.pid)
            trial["stats_after_measured"] = server_stats(client, engine)
            after = check("after_measured")
            if name == "incr":
                delta = after["counter_sum"] - before["counter_sum"]
                trial["measured_counter_increments"] = delta
                if delta != count:
                    raise AssertionError(f"INCR sum delta {delta}, requested {count}")
            if not measured["ok"]:
                raise RuntimeError("measured phase failed; raw evidence retained")
    except (Exception, KeyboardInterrupt) as error:
        trial["errors"].append(f"{type(error).__name__}: {error}")
        trial["interrupted"] = isinstance(error, KeyboardInterrupt)
    finally:
        trial["server_command"] = server.command_line
        trial["server_log"] = server.diagnostics()
        trial["server_log_limit_bytes"] = 65536
        process = server.process
        try:
            server.close()
        except Exception as error:
            trial["errors"].append(f"cleanup: {type(error).__name__}: {error}")
        trial["server_exit_code"] = process.returncode if process is not None else None
    trial["ok"] = not trial["errors"]
    trial["status"] = "pass" if trial["ok"] else "fail"
    return trial


def summarize(trials):
    groups = {}
    for trial in trials:
        group_key = (trial["engine"], trial["mode"], trial["workload"], trial["profile"])
        group = groups.setdefault(group_key, {"engine": group_key[0], "mode": group_key[1],
                                            "workload": group_key[2], "profile": group_key[3],
                                            "successful_trials": 0, "failed_trials": 0,
                                            "unsupported_trials": 0,
                                            "short_of_target_trials": 0, "subsecond_trials": 0,
                                            "native_requests_per_second": [], "wall_seconds": []})
        if trial["status"] == "unsupported":
            group["unsupported_trials"] += 1
            group["unsupported"] = trial["unsupported"]
            continue
        if not trial["ok"]:
            group["failed_trials"] += 1
            continue
        measured = next(phase for phase in trial["phases"] if phase["phase"] == "measured")
        group["successful_trials"] += 1
        group["native_requests_per_second"].append(measured["csv"]["requests_per_second"])
        group["wall_seconds"].append(measured["wall_seconds"])
        group["short_of_target_trials"] += not trial["duration_quality"]["target_met"]
        group["subsecond_trials"] += not trial["duration_quality"]["sustained_at_least_one_second"]
    for group in groups.values():
        values = group["native_requests_per_second"]
        group["median_native_requests_per_second"] = statistics.median(values) if values else None
        group["complete"] = group["failed_trials"] == 0 and group["unsupported_trials"] == 0
        # Unsupported is neither pass nor fail and is never comparable as a result.
        group["status"] = ("fail" if group["failed_trials"] else
                           "unsupported" if group["unsupported_trials"] else "pass")
    return list(groups.values())


def client_version_check(observed):
    match = re.search(r"redis-benchmark\s+(\S+)", observed.get("stdout", ""))
    found = match.group(1) if match else None
    return {"expected": NATIVE_CLIENT_VERSION, "observed": found,
            "latency_semantics_source_matches": found == NATIVE_CLIENT_VERSION,
            "note": "mismatch is recorded, not fatal; LATENCY_SEMANTICS are source-verified only for the expected version"}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", default="target/release/ffkv", help="ffkv executable (memory storage for every mode)")
    parser.add_argument("--redis-binary", default="redis-server", help="owned Redis server executable")
    parser.add_argument("--dragonfly-binary", default="dragonfly",
                        help="owned Dragonfly server executable (only resolved when selected)")
    parser.add_argument("--benchmark-binary", default="redis-benchmark", help="compiled redis-benchmark executable")
    parser.add_argument("--engines", nargs="+", choices=ENGINE_KINDS, default=["ffkv", "redis"],
                        help="engines in repetition-0 order; rotated by one each repetition (default: ffkv redis)")
    parser.add_argument("--ffkv-shards", type=int, default=4, help="ffkv --shards (default: 4)")
    parser.add_argument("--dragonfly-threads", type=int, default=4,
                        help="Dragonfly --proactor_threads (default: 4)")
    parser.add_argument("--server-cpus", type=cpu_list_argument, default=None,
                        help="Linux only: taskset cpu list for every owned server, e.g. 0-3")
    parser.add_argument("--client-cpus", type=cpu_list_argument, default=None,
                        help="Linux only: taskset cpu list for redis-benchmark, disjoint from --server-cpus")
    parser.add_argument("--workloads", nargs="+", choices=WORKLOADS, default=list(WORKLOADS),
                        help="memory workload selection; durability modes only run selected set-256")
    parser.add_argument("--profiles", nargs="+", choices=PROFILES,
                        default=["c1-p1-t1", "c32-p1-t4", "c32-p16-t4"],
                        help="c=connections, p=pipeline depth, t=native client threads")
    parser.add_argument("--modes", nargs="+", choices=("memory", "everysec", "strict"),
                        default=["memory", "everysec", "strict"])
    parser.add_argument("--repetitions", type=int, default=3, help="measured repetitions per case (default: 3)")
    parser.add_argument("--target-seconds", type=float, default=3, help="approximate measured native duration (default: 3)")
    parser.add_argument("--calibration-iterations", type=int, default=6,
                        help="maximum excluded pilot rounds; first uses 2000-5000 requests (default: 6)")
    parser.add_argument("--min-requests", type=int, default=128, help="lower measured request bound before profile alignment")
    parser.add_argument("--max-requests", type=int, default=5000000, help="hard upper measured request bound (default: 5000000)")
    parser.add_argument("--seed", type=int, default=12345, help="explicit identical native random seed for every phase")
    parser.add_argument("--timeout", type=float, default=120, help="hard timeout per native phase in seconds")
    parser.add_argument("--startup-timeout", type=float, default=20, help="owned server startup/shutdown timeout")
    parser.add_argument("--socket-timeout", type=float, default=30, help="RESP preload/validation socket timeout")
    parser.add_argument("--preload-timeout", type=float, default=180, help="overall bounded preload/counter-scan deadline")
    parser.add_argument("--json", default="redis-comparison.json", help="JSON evidence destination; '-' writes stdout")
    args = parser.parse_args()
    for name in ("target_seconds", "timeout", "startup_timeout", "socket_timeout", "preload_timeout"):
        value = getattr(args, name)
        if not math.isfinite(value) or value <= 0:
            parser.error(f"--{name.replace('_', '-')} must be finite and positive")
    if args.repetitions < 1 or not 1 <= args.min_requests <= args.max_requests <= 100000000:
        parser.error("positive repetitions and 1 <= min-requests <= max-requests <= 100000000 required")
    if not 1 <= args.calibration_iterations <= 12:
        parser.error("--calibration-iterations must be between 1 and 12")
    if not 0 <= args.seed <= 2147483647:
        parser.error("--seed must be between 0 and 2147483647")
    if "memory" not in args.modes and "set-256" not in args.workloads:
        parser.error("durable modes require set-256 in --workloads")
    if len(set(args.engines)) != len(args.engines):
        parser.error("--engines must not repeat an engine")
    if not 1 <= args.ffkv_shards <= 256:
        parser.error("--ffkv-shards must be between 1 and 256")
    if not 1 <= args.dragonfly_threads <= 1024:
        parser.error("--dragonfly-threads must be between 1 and 1024")
    error = affinity_error(args.server_cpus, args.client_cpus)
    if error:
        parser.error(error)
    for profile in args.profiles:
        try:
            aligned_count(args.min_requests, profile, args.min_requests, args.max_requests)
        except ValueError as error:
            parser.error(str(error))
    return args


def main():
    args = parse_args()
    unsupported = sorted({(engine, mode) for engine in args.engines for mode in args.modes
                          if mode not in ENGINE_MODES[engine]})
    for engine, mode in unsupported:
        print(f"unsupported: {engine} {mode}; recorded as unsupported trials, not executed",
              file=sys.stderr, flush=True)
    report = {"schema_version": 1, "ok": False, "environment": host_environment(), "settings": vars(args).copy(),
              "started_unix_seconds": time.time(), "trials": [], "errors": [], "versions": {},
              "unsupported_combinations": [{"engine": engine, "mode": mode, **UNSUPPORTED[(engine, mode)]}
                                           for engine, mode in unsupported],
              "methodology": {"load_generator": "compiled redis-benchmark; Python only orchestrates",
                              "isolation": "fresh owned child and temporary directory per engine/case/repetition",
                              "engine_order": "--engines order on repetition 0, rotated left by one per repetition; two engines alternate",
                              "engine_order_balanced": args.repetitions % len(args.engines) == 0,
                              "random_keys": "native-bench: plus 12 zero-padded digits; uniform native -r domain",
                              "writes": "SET/HSET vary a fixed-width numeric payload suffix; ZADD varies the existing member score, rather than timing only same-value no-ops",
                              "latency": LATENCY_SEMANTICS,
                              "native_semantics_source": "https://raw.githubusercontent.com/redis/redis/8.2.1/src/redis-benchmark.c",
                              "native_throughput_caveat": "Redis 8.2.1 threaded completion detection uses a 250ms timer; short-pilot rps is quantized",
                              "excluded": ["startup", "preload", "warmup", "calibration", "validation", "stats", "shutdown"],
                              "calibration": "2000-5000 request pilot, separate 2000-5000 request warmup",
                              "calibration_refinement": "bounded additional pilots up to one native second, at most calibration-iterations",
                              "maintenance": "ffkv automatic WAL checkpoints and Redis auto AOF rewrites disabled",
                              "ffkv_shards": args.ffkv_shards,
                              "dragonfly": {"proactor_threads": args.dragonfly_threads,
                                            "memory_mode": "--dbfilename= (no load/shutdown save), --snapshot_cron= (no periodic snapshot), --version_check=false, --logtostderr",
                                            "other_flags": "Dragonfly defaults (pipeline_squash, maxmemory auto, epoll/io_uring auto)",
                                            "durable_modes": "unsupported; recorded, never executed, never counted as pass"},
                              "redis": "single-threaded default io-threads; no thread tuning",
                              "cpu_affinity": {"server_cpus": args.server_cpus, "client_cpus": args.client_cpus,
                                               "method": "taskset -c prefix execs into the target, so the owned PID is the engine; server affinity verified with sched_getaffinity",
                                               "client_verification": "taskset failure is a native nonzero exit; client PID affinity not resampled"},
                              "durability": "acknowledged-write benchmark; no crash-recovery or equal guarantee claim",
                              "cpu": "server cumulative CPU around native process; includes startup/client handshake interval",
                              "seed_limit": "fixed seed does not promise identical interleaving across native client threads"}}
    started = time.monotonic()
    try:
        fields = {"ffkv": "binary", "redis": "redis_binary", "dragonfly": "dragonfly_binary"}
        for field in [fields[engine] for engine in ENGINE_KINDS if engine in args.engines] + ["benchmark_binary"]:
            candidate = getattr(args, field)
            resolved = shutil.which(candidate)
            if resolved is None:
                raise FileNotFoundError(f"{field}: executable not found: {candidate}")
            setattr(args, field, str(Path(resolved).resolve()))
            report["versions"][field] = version(getattr(args, field))
        report["client_version_check"] = client_version_check(report["versions"]["benchmark_binary"])
        if not report["client_version_check"]["latency_semantics_source_matches"]:
            report["environment"]["limitations"].append(
                f"redis-benchmark is not {NATIVE_CLIENT_VERSION}; latency semantics are unverified for this client.")
        report["resolved_settings"] = vars(args).copy()
        for repetition in range(args.repetitions):
            shift = repetition % len(args.engines)
            order = args.engines[shift:] + args.engines[:shift]
            for mode in args.modes:
                for name in args.workloads:
                    if mode != "memory" and name != "set-256":
                        continue
                    for profile in args.profiles:
                        for engine in order:
                            if mode not in ENGINE_MODES[engine]:
                                report["trials"].append(unsupported_trial(engine, mode, name, profile, repetition, order))
                                continue
                            print(f"[{repetition + 1}/{args.repetitions}] {engine} {mode} {name} {profile}",
                                  file=sys.stderr, flush=True)
                            with tempfile.TemporaryDirectory(prefix="ffkv-native-compare-") as directory:
                                trial = run_trial(args, engine, mode, name, profile, repetition, order, directory)
                            report["trials"].append(trial)
                            report["summary"] = summarize(report["trials"])
                            # Checkpoint partial evidence, including every failed case.
                            if args.json != "-":
                                write_report(report, args.json)
                            if trial.get("interrupted"):
                                raise KeyboardInterrupt()
        report["ok"] = bool(report["trials"]) and all(trial["status"] == "pass" for trial in report["trials"])
    except (Exception, KeyboardInterrupt) as error:
        report["errors"].append(f"{type(error).__name__}: {error}")
        report["ok"] = False
    finally:
        report["elapsed_seconds"] = time.monotonic() - started
        report["summary"] = summarize(report["trials"])
        report["failed_trials"] = sum(trial["status"] == "fail" for trial in report["trials"])
        report["unsupported_trials"] = sum(trial["status"] == "unsupported" for trial in report["trials"])
        report["passed_trials"] = sum(trial["status"] == "pass" for trial in report["trials"])
        report["status"] = ("pass" if report["ok"] else
                            "fail" if report["failed_trials"] or report["errors"] or not report["trials"] else
                            "unsupported")
        write_report(report, args.json)
    return 0 if report["status"] == "pass" else 3 if report["status"] == "unsupported" else 1


if __name__ == "__main__":
    sys.exit(main())
