"""Standard-library RESP, owned-process and current-RSS helpers for real workloads."""
import argparse
import contextlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time


class ResponseError(RuntimeError):
    pass


class Client:
    def __init__(self, address, timeout=10):
        self.socket = socket.create_connection(address, timeout=timeout)
        self.socket.settimeout(timeout)
        self.file = self.socket.makefile("rb")

    def close(self):
        with contextlib.suppress(OSError):
            self.socket.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self.file.close()
        with contextlib.suppress(OSError):
            self.socket.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @staticmethod
    def frame(args):
        parts = [x if isinstance(x, bytes) else str(x).encode() for x in args]
        return b"*%d\r\n" % len(parts) + b"".join(
            b"$%d\r\n" % len(x) + x + b"\r\n" for x in parts)

    def read(self):
        line = self.file.readline(65537)
        if not line.endswith(b"\r\n"):
            raise ConnectionError("truncated or oversized RESP header")
        kind, value = line[:1], line[1:-2]
        if kind == b"-":
            return ResponseError(value.decode(errors="replace"))
        if kind == b"+":
            return value
        if kind == b":":
            return int(value)
        if kind == b"_":
            return None
        if kind == b"#":
            return value == b"t"
        if kind == b"$":
            size = int(value)
            if size == -1:
                return None
            if not 0 <= size <= 16 * 1024 * 1024:
                raise ValueError("unsupported RESP bulk size")
            result = self.file.read(size)
            if len(result) != size or self.file.read(2) != b"\r\n":
                raise ConnectionError("truncated RESP body")
            return result
        if kind in (b"*", b"~", b"%"):
            size = int(value)
            if size == -1:
                return None
            if not 0 <= size <= 1000000:
                raise ValueError("unsupported RESP array size")
            if kind == b"%":
                return {self.read(): self.read() for _ in range(size)}
            return [self.read() for _ in range(size)]
        raise ValueError("unsupported RESP type: " + repr(kind))

    def command(self, *args):
        self.socket.sendall(self.frame(args))
        result = self.read()
        if isinstance(result, ResponseError):
            raise result
        return result


def mapping(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, list) or len(value) % 2:
        raise AssertionError("expected RESP map")
    return dict(zip(value[::2], value[1::2]))


def jsonable(value):
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    if isinstance(value, dict):
        return {str(jsonable(k)): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    return value


def current_rss(pid):
    """Resident bytes now, never ru_maxrss; unavailable is explicit, not zero."""
    try:
        if platform.system() == "Linux":
            fields = Path(f"/proc/{pid}/statm").read_text().split()
            return {"bytes": int(fields[1]) * os.sysconf("SC_PAGE_SIZE"),
                    "source": "/proc/PID/statm resident pages"}
        result = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                                capture_output=True, text=True, timeout=3, check=True)
        return {"bytes": int(result.stdout.strip()) * 1024, "source": "ps current RSS KiB"}
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        return {"bytes": None, "source": "unavailable", "error": str(error)}


def environment():
    runtime_variables = ("TOKIO_WORKER_THREADS", "LD_PRELOAD", "LD_LIBRARY_PATH",
                         "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH",
                         "MALLOC_CONF", "GLIBC_TUNABLES")
    return {"os": platform.platform(), "machine": platform.machine(),
            "python": platform.python_version(), "client": "stdlib RESP2 runtime.py",
            "cpu_count": os.cpu_count(),
            "runtime_environment": {name: os.environ[name] for name in runtime_variables
                                    if name in os.environ}}


def version(binary):
    try:
        result = subprocess.run([str(binary), "--version"], capture_output=True,
                                text=True, timeout=5)
        return {"command": [str(binary), "--version"], "exit_code": result.returncode,
                "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}
    except (OSError, subprocess.SubprocessError) as error:
        return {"error": str(error)}


def parse_cpu_list(text):
    """Parse a Linux cpu list such as '0-3,8'; reject empty, reversed or duplicate ids."""
    cpus = []
    for part in str(text).split(","):
        part = part.strip()
        match = re.fullmatch(r"(\d+)(?:-(\d+))?", part)
        if not match:
            raise ValueError(f"invalid cpu list element: {part!r}")
        low = int(match.group(1))
        high = int(match.group(2)) if match.group(2) is not None else low
        if high < low:
            raise ValueError(f"reversed cpu range: {part!r}")
        cpus.extend(range(low, high + 1))
    if not cpus or len(set(cpus)) != len(cpus):
        raise ValueError(f"empty or duplicate cpu list: {text!r}")
    return sorted(cpus)


def cpu_list_argument(text):
    """argparse type wrapper for parse_cpu_list."""
    try:
        return parse_cpu_list(text)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error))


def affinity_error(server_cpus, client_cpus):
    """Validate optional Linux pinning before any process (including this one) is pinned."""
    if server_cpus is None and client_cpus is None:
        return None
    if platform.system() != "Linux":
        return "--server-cpus/--client-cpus require Linux taskset"
    if shutil.which("taskset") is None:
        return "--server-cpus/--client-cpus require taskset on PATH"
    allowed = os.sched_getaffinity(0)
    for flag, cpus in (("--server-cpus", server_cpus), ("--client-cpus", client_cpus)):
        if cpus is not None and not set(cpus) <= allowed:
            return f"{flag} {cpus} outside harness affinity {sorted(allowed)}"
    if server_cpus and client_cpus and set(server_cpus) & set(client_cpus):
        return "--server-cpus and --client-cpus must be disjoint"
    return None


def linux_observations():
    """Harness affinity, cgroup CPU/memory limits, kernel and CPU model; never fatal."""
    linux = {"harness_affinity": sorted(os.sched_getaffinity(0))}
    for name, path in (("kernel", "/proc/sys/kernel/osrelease"),
                       ("cgroup_cpuset_effective", "/sys/fs/cgroup/cpuset.cpus.effective"),
                       ("cgroup_cpu_max", "/sys/fs/cgroup/cpu.max"),
                       ("cgroup_memory_max", "/sys/fs/cgroup/memory.max")):
        try:
            linux[name] = Path(path).read_text().strip()
        except OSError as error:
            linux[name] = {"unavailable": str(error)}
    try:
        models = {line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                  if line.lower().startswith(("model name", "cpu part", "hardware"))}
        linux["cpuinfo_models"] = sorted(models)
    except OSError as error:
        linux["cpuinfo_models"] = {"unavailable": str(error)}
    return linux


def affinity_prefix(cpus):
    """taskset sets affinity then execs, so the Popen PID becomes the target program."""
    if cpus is None:
        return []
    if platform.system() != "Linux":
        raise RuntimeError("CPU affinity requires Linux taskset")
    taskset = shutil.which("taskset")
    if taskset is None:
        raise FileNotFoundError("taskset executable not found")
    return [taskset, "-c", ",".join(str(cpu) for cpu in cpus)]


ENGINE_KINDS = ("ffkv", "redis", "dragonfly")
ENGINE_MODES = {"ffkv": ("memory", "everysec", "strict"),
                "redis": ("memory", "everysec", "strict"),
                "dragonfly": ("memory",)}
UNSUPPORTED = {
    ("dragonfly", mode): {
        "reason": ("Dragonfly has no append-only file or per-write/per-second local fsync; "
                   "point-in-time snapshots (SAVE/BGSAVE, --snapshot_cron, --dbfilename) and "
                   "the in-memory replication journal are not equivalent to " + mode
                   + " acknowledged-write durability, so no comparison is run."),
        "evidence": [
            "https://www.dragonflydb.io/docs/managing-dragonfly/flags (dbfilename, dir, snapshot_cron; no appendonly/appendfsync flags)",
            "https://github.com/dragonflydb/dragonfly/blob/v2.0.2/src/server/server_family.cc (flags dir/dbfilename/snapshot_cron; Shutdown saves only when dbfilename is non-empty)",
        ]}
    for mode in ("everysec", "strict")}


def dragonfly_args(threads):
    """The only Dragonfly tuning flag; memory-mode flags are fixed in OwnedServer."""
    return [f"--proactor_threads={threads}"]


def dragonfly_settings(client, server, threads):
    """Memory-only Dragonfly: snapshot load/save and periodic snapshots disabled, owned dir."""
    expected = {"dbfilename": "", "snapshot_cron": "", "dir": str(server.directory)}
    result = {"expected": expected, "actual": {}, "ok": False,
              "source": "CONFIG GET of registered flags, INFO persistence, INFO server thread_count, owned CLI",
              "snapshot_on_shutdown": "disabled: Shutdown saves only when dbfilename is non-empty",
              "durability": "none; no crash-recovery claim"}
    for setting in expected:
        reply = client.command("CONFIG", "GET", setting)
        if not isinstance(reply, list) or len(reply) != 2 or reply[0] != setting.encode():
            raise AssertionError(f"CONFIG GET {setting}: {reply!r}")
        result["actual"][setting] = reply[1].decode()
    raw = client.command("INFO", "persistence")
    result["info_persistence"] = info_fields(raw)
    observed = (server.identity or {}).get("thread_count")
    result["proactor_threads"] = {
        "requested": threads, "info_server_thread_count": observed, "source": "INFO server thread_count",
        "status": ("verified" if observed is not None and observed.strip().isdigit() and int(observed) == threads
                   else "mismatch" if observed is not None else "missing")}
    result["ok"] = (result["actual"] == expected
                    and result["info_persistence"].get("rdb_bgsave_in_progress", "0") == "0"
                    and result["proactor_threads"]["status"] == "verified")
    return result


class OwnedServer:
    """Only signal our Popen child; all logs and data live in an owned directory.

    Redis and Dragonfly must report INFO server process_id equal to the spawned PID,
    so a reserved port that was raced by another listener is rejected, never used.
    """
    def __init__(self, binary, directory, kind="ffkv", mode="memory", storage="memory",
                 timeout=15, extra_args=(), cpus=None):
        if kind not in ENGINE_KINDS:
            raise ValueError(f"unknown server kind: {kind}")
        if kind == "dragonfly" and mode != "memory":
            raise ValueError("dragonfly has no local append-only fsync mode; only memory is supported")
        self.binary = str(binary)
        self.directory = Path(directory)
        self.kind, self.mode, self.storage = kind, mode, storage
        self.timeout = timeout
        self.extra_args = list(extra_args)
        self.cpus = None if cpus is None else sorted(cpus)
        self.process = None
        self.log = None
        self.address = None
        self.command_line = None
        self.server_args = None
        self.identity = None
        self.affinity = None

    def start(self):
        self.directory.mkdir(parents=True, exist_ok=True)
        if self.kind == "ffkv":
            args = [self.binary, "--bind", "127.0.0.1:0", "--storage", self.storage]
            if self.mode != "memory":
                args += ["--data-dir", str(self.directory / "data"), "--durability", self.mode]
            args += self.extra_args
        else:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            self.address = ("127.0.0.1", port)
            if self.kind == "redis":
                args = [self.binary, "--bind", "127.0.0.1", "--port", str(port),
                        "--protected-mode", "yes", "--dir", str(self.directory),
                        "--save", "", "--appendonly", "no" if self.mode == "memory" else "yes",
                        "--appendfsync", "always" if self.mode == "strict" else "everysec",
                        "--no-appendfsync-on-rewrite", "no", "--auto-aof-rewrite-percentage", "0"]
            else:
                # Empty dbfilename disables both startup load and shutdown save; empty
                # snapshot_cron disables periodic snapshots; version_check avoids network I/O.
                args = [self.binary, "--bind=127.0.0.1", f"--port={port}",
                        f"--dir={self.directory}", "--dbfilename=", "--snapshot_cron=",
                        "--version_check=false", "--logtostderr"]
            args += self.extra_args
        self.server_args = args
        self.command_line = affinity_prefix(self.cpus) + args
        self.log = tempfile.TemporaryFile(mode="w+b")
        try:
            self.process = subprocess.Popen(self.command_line, stdout=self.log, stderr=self.log)
            deadline = time.monotonic() + self.timeout
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError("server exited during startup: " + self.diagnostics())
                if self.kind == "ffkv":
                    text = self.diagnostics()
                    match = re.search(r"listening on (?:127\.0\.0\.1|localhost):(\d+)", text)
                    if match:
                        self.address = ("127.0.0.1", int(match.group(1)))
                if self.address:
                    try:
                        with Client(self.address, min(self.timeout, 1)) as client:
                            if client.command("PING") != b"PONG":
                                raise RuntimeError("unexpected startup PING")
                            if self.kind != "ffkv":
                                info = info_fields(client.command("INFO", "server"))
                                if int(info.get("process_id", -1)) != self.process.pid:
                                    raise RuntimeError(f"port is not owned by spawned {self.kind}")
                                is_dragonfly = "dragonfly_version" in info
                                if is_dragonfly != (self.kind == "dragonfly") or "valkey_version" in info:
                                    raise RuntimeError(f"INFO server identity does not match {self.kind}")
                                self.identity = info
                    except (OSError, ConnectionError):
                        time.sleep(0.02)
                        continue
                    # Readiness implies exec completed; verification errors are fatal, not retried.
                    self.affinity = self.verify_affinity()
                    return self
                time.sleep(0.02)
            raise TimeoutError("server startup timed out: " + self.diagnostics())
        except BaseException:
            self.close()
            raise

    def verify_affinity(self):
        """Linux: the PID must have exec'd the server binary with exactly the requested CPUs."""
        pid = self.process.pid
        if platform.system() != "Linux":
            return {"requested": None, "effective": None, "source": "unavailable off Linux"}
        exe = os.path.realpath(f"/proc/{pid}/exe")
        if exe != os.path.realpath(self.binary):
            raise RuntimeError(f"owned PID executes {exe}, expected {self.binary}")
        effective = sorted(os.sched_getaffinity(pid))
        if self.cpus is not None and effective != self.cpus:
            raise RuntimeError(f"server affinity {effective}, requested {self.cpus}")
        return {"requested": self.cpus, "effective": effective, "exe": exe,
                "source": "os.sched_getaffinity(owned PID) after readiness"}

    def diagnostics(self):
        if self.log is None:
            return ""
        return os.pread(self.log.fileno(), 65536, 0).decode(errors="replace")

    def close(self, crash=False):
        process = self.process
        try:
            if process is not None:
                if process.poll() is None:
                    with contextlib.suppress(ProcessLookupError):
                        process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
                try:
                    code = process.wait(timeout=self.timeout)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=self.timeout)
                    raise TimeoutError("owned server required forced shutdown")
                expected = -signal.SIGKILL if crash else 0
                if code != expected:
                    raise RuntimeError(f"server exit {code}, expected {expected}: {self.diagnostics()}")
        finally:
            self.process = None
            if self.log is not None:
                self.log.close()
                self.log = None
            self.address = None

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()


def info_fields(raw):
    return dict(line.split(":", 1) for line in raw.decode(errors="replace").splitlines()
                if ":" in line and not line.startswith("#"))


class Histogram:
    """Bounded measured latency histogram; quantiles are explicit bucket upper bounds."""
    def __init__(self):
        self.buckets = {}
        self.count = 0
        self.maximum = 0.0

    def add(self, seconds):
        micros = max(seconds * 1e6, 0.001)
        bucket = math.ceil(math.log2(micros) * 1024)
        self.buckets[bucket] = self.buckets.get(bucket, 0) + 1
        self.count += 1
        self.maximum = max(self.maximum, seconds)

    def merge(self, other):
        for k, count in other.buckets.items():
            self.buckets[k] = self.buckets.get(k, 0) + count
        self.count += other.count
        self.maximum = max(self.maximum, other.maximum)

    def report(self):
        result = {"latency_samples": self.count,
                  "percentile_method": "measured logarithmic histogram upper bound; bucket ratio 2^(1/1024)",
                  "max_ms": self.maximum * 1000 if self.count else None}
        for name, fraction in (("p50_ms", .50), ("p95_ms", .95), ("p99_ms", .99)):
            target, total = math.ceil(self.count * fraction), 0
            result[name] = None
            for bucket, count in sorted(self.buckets.items()):
                total += count
                if total >= target:
                    result[name] = 2 ** (bucket / 1024) / 1000
                    break
        return result


def write_report(report, destination):
    text = json.dumps(jsonable(report), indent=2, sort_keys=True, allow_nan=False) + "\n"
    if destination == "-":
        print(text, end="")
    else:
        Path(destination).write_text(text)
