"""Standard-library RESP, owned-process and current-RSS helpers for real workloads."""
import contextlib
import json
import math
import os
from pathlib import Path
import platform
import re
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
    return {"os": platform.platform(), "machine": platform.machine(),
            "python": platform.python_version(), "client": "stdlib RESP2 runtime.py", 
            "cpu_count": os.cpu_count()}


def version(binary):
    try:
        result = subprocess.run([str(binary), "--version"], capture_output=True,
                                text=True, timeout=5)
        return {"command": [str(binary), "--version"], "exit_code": result.returncode,
                "stdout": result.stdout.strip(), "stderr": result.stderr.strip()}
    except (OSError, subprocess.SubprocessError) as error:
        return {"error": str(error)}


class OwnedServer:
    """Only signal our Popen child; all logs and data live in an owned directory."""
    def __init__(self, binary, directory, kind="ffkv", mode="memory", storage="memory",
                 timeout=15, extra_args=()):
        self.binary = str(binary)
        self.directory = Path(directory)
        self.kind, self.mode, self.storage = kind, mode, storage
        self.timeout = timeout
        self.extra_args = list(extra_args)
        self.process = None
        self.log = None
        self.address = None
        self.command_line = None

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
            args = [self.binary, "--bind", "127.0.0.1", "--port", str(port),
                    "--protected-mode", "yes", "--dir", str(self.directory),
                    "--save", "", "--appendonly", "no" if self.mode == "memory" else "yes",
                    "--appendfsync", "always" if self.mode == "strict" else "everysec",
                    "--no-appendfsync-on-rewrite", "no", "--auto-aof-rewrite-percentage", "0"]
        self.command_line = args
        self.log = tempfile.TemporaryFile(mode="w+b")
        try:
            self.process = subprocess.Popen(args, stdout=self.log, stderr=self.log)
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
                                    raise RuntimeError("port is not owned by spawned Redis")
                        return self
                    except (OSError, ConnectionError):
                        pass
                time.sleep(0.02)
            raise TimeoutError("server startup timed out: " + self.diagnostics())
        except BaseException:
            self.close()
            raise

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
