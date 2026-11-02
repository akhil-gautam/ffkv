#!/usr/bin/env python3
"""Exercise ffkv WAL persistence with real processes, sockets, and owned fixtures.

Usage: python3 tests/durability_smoke.py [--binary target/debug/ffkv]

Every persistent scenario owns a TemporaryDirectory. Deliberate WAL damage is
applied only to copies of a stopped fixture inside that scenario's directory.
SIGKILL exercises process-crash recovery, not power-loss durability or HA. The
idle everysec case exercises the timer's real path but cannot independently
observe a storage device's fsync behavior. No Redis installation is required.
"""
import argparse
import contextlib
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import tempfile
import time

from cli_smoke import Client
from queue_smoke import IO_TIMEOUT, begin_wait, check, check_error, startup_address

try:
    import resource
except ImportError:
    resource = None


WAL_NAME = "wal.ffkv"
FILE_MAGIC = b"FFKVWAL1"
RECORD_MAGIC = b"FREC"
HEADER_SIZE = 24


def passed(message):
    print(f"PASS {message}", flush=True)


class Server:
    """Own one process and its clients, including cleanup when startup fails."""

    def __init__(self, binary, directory=None, durability=None, file_limit=None, extra_args=()):
        self.binary = binary
        self.directory = directory
        self.durability = durability
        self.file_limit = file_limit
        self.extra_args = tuple(extra_args)
        self.process = None
        self.clients = []
        self.address = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            if self.process is not None:
                self.stop()
        else:
            self.cleanup()

    def start(self):
        assert self.process is None, "server is already running"
        args = [self.binary, "--bind", "127.0.0.1:0"]
        if self.directory is not None:
            args += ["--data-dir", str(self.directory)]
        if self.durability is not None:
            args += ["--durability", self.durability]
        args.extend(self.extra_args)
        options = {}
        if self.file_limit is not None:
            limit = self.file_limit

            def limit_file_size():
                # Keep the ignored disposition across exec so an oversized WAL
                # write returns EFBIG instead of terminating through SIGXFSZ.
                signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
                resource.setrlimit(resource.RLIMIT_FSIZE, (limit, limit))

            options = {"preexec_fn": limit_file_size, "restore_signals": False}
        self.process = subprocess.Popen(
            args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, **options,
        )
        try:
            self.address = startup_address(self.process)
            check(self.connect(), ["PING"], b"PONG")
        except BaseException:
            self.cleanup()
            raise

    def connect(self):
        assert self.process is not None and self.process.poll() is None
        client = Client(self.address)
        client.socket.settimeout(IO_TIMEOUT)
        self.clients.append(client)
        return client

    def close_clients(self):
        for client in self.clients:
            with contextlib.suppress(OSError):
                client.close()
        self.clients.clear()

    def release_process(self):
        self.close_clients()
        if self.process is not None and self.process.stderr is not None:
            self.process.stderr.close()
        self.process = None
        self.address = None

    def stop(self, sig=signal.SIGINT):
        process = self.process
        assert process is not None
        if process.poll() is None:
            process.send_signal(sig)
        try:
            code = process.wait(timeout=IO_TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=IO_TIMEOUT)
            self.release_process()
            raise AssertionError(f"ffkv did not stop within {IO_TIMEOUT}s") from None
        diagnostics = process.stderr.read(65536)
        self.release_process()
        expected = -signal.SIGKILL if sig == signal.SIGKILL else 0
        assert code == expected, ("ffkv shutdown", code, expected, diagnostics)

    def expect_failure(self):
        process = self.process
        assert process is not None
        try:
            code = process.wait(timeout=IO_TIMEOUT)
        except subprocess.TimeoutExpired:
            raise AssertionError("WAL write failure did not fail-stop ffkv") from None
        diagnostics = process.stderr.read(65536)
        self.release_process()
        assert code != 0, ("failed WAL write exited successfully", diagnostics)
        assert code != -signal.SIGXFSZ, (
            "file-size signal was not ignored; WAL error handling was not exercised",
            diagnostics,
        )

    def cleanup(self):
        process = self.process
        if process is not None and process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                process.send_signal(signal.SIGINT)
            try:
                process.wait(timeout=IO_TIMEOUT)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=IO_TIMEOUT)
        self.release_process()


def startup_failure(binary, directory):
    """A failure must be finite and must never reach the listening state."""
    process = subprocess.Popen(
        [binary, "--bind", "127.0.0.1:0", "--data-dir", str(directory)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    try:
        try:
            _, diagnostics = process.communicate(timeout=IO_TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            _, diagnostics = process.communicate(timeout=IO_TIMEOUT)
            raise AssertionError(("expected startup failure, process stayed alive", diagnostics)) from None
        assert process.returncode != 0, ("startup unexpectedly succeeded", diagnostics)
        assert b"listening on " not in diagnostics, ("invalid startup accepted clients", diagnostics)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=IO_TIMEOUT)
        process.stderr.close()


def current_ms():
    return time.time_ns() // 1_000_000


def wait_until_ms(deadline):
    # Only TTL tests wait on wall time; the watchdog itself is monotonic.
    watchdog = time.monotonic() + IO_TIMEOUT
    while current_ms() < deadline:
        remaining = (deadline - current_ms()) / 1000
        assert time.monotonic() < watchdog, "wall-clock TTL wait exceeded watchdog"
        time.sleep(min(max(remaining, 0.001), 0.05))


def check_deadline(client, key, deadline):
    before = current_ms()
    ttl = client.command("PTTL", key)
    after = current_ms()
    assert isinstance(ttl, int) and ttl > 0, (key, ttl)
    assert deadline - after - 100 <= ttl <= deadline - before + 100, (
        key, "absolute expiry changed on replay", deadline, before, after, ttl,
    )


def strings_and_lists(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-values-") as directory:
        binary_key = b"key\x00\xff\r\n"
        binary_value = b"\x00\xff\r\n$3\r\nabc\x00"
        list_key = b"list\x00\xff\r\n"
        expected_strings = {
            binary_key: binary_value, b"": b"", b"empty-value": b"",
            b"replaced-list": b"now-string", b"mset-list": b"mset-string",
            b"multi:a": b"a", b"multi:b": b"b", b"duplicate": b"last",
            b"conditional": b"second", b"created-nx": b"created",
            b"arithmetic": b"40", b"overflow": b"9223372036854775807",
            b"persist": b"permanent", b"mset-ttl": b"no-ttl",
        }

        def verify(client):
            for key, value in expected_strings.items():
                check(client, ["GET", key], value)
            for key in (b"deleted", b"deleted:other", b"conditional-missing", b"popped-empty"):
                check(client, ["TYPE", key], b"none")
            check(client, ["LRANGE", list_key, 0, -1], [b"middle\x00\xff\r\n", b"right", b""])
            check(client, ["LRANGE", "replaced-string", 0, -1], [b"left", b"right"])
            check(client, ["TYPE", "replaced-list"], b"string")
            check(client, ["TYPE", "replaced-string"], b"list")
            check(client, ["PTTL", "persist"], -1)
            check(client, ["PTTL", "mset-ttl"], -1)

        # Deliberately omit --durability: a data directory defaults to strict.
        with Server(binary, directory) as server:
            client = server.connect()
            check(client, ["SET", binary_key, binary_value], b"OK")
            check(client, ["SET", b"", b""], b"OK")
            check(client, ["SET", "empty-value", b""], b"OK")
            check(client, ["RPUSH", "replaced-list", "old-a", "old-b"], 2)
            check(client, ["SET", "replaced-list", "now-string"], b"OK")
            check_error(client, ["RPUSH", "replaced-list", "rejected"], b"WRONGTYPE")
            check(client, ["SET", "replaced-string", "old-string"], b"OK")
            check(client, ["DEL", "replaced-string"], 1)
            check(client, ["RPUSH", "replaced-string", "left", "right"], 2)
            check(client, ["RPUSH", "mset-list", "old"], 1)
            check(client, ["MSET", "multi:a", "a", "multi:b", "b", "duplicate", "first",
                           "duplicate", "last", "mset-list", "mset-string"], b"OK")
            check(client, ["MSET", "deleted", "gone", "deleted:other", "also-gone"], b"OK")
            check(client, ["DEL", "deleted", "deleted:other", "deleted", "absent"], 2)
            check(client, ["SET", "conditional", "first", "NX"], b"OK")
            check(client, ["SET", "conditional", "rejected", "NX"], None)
            check(client, ["SET", "conditional", "second", "XX", "GET"], b"first")
            check(client, ["SET", "conditional-missing", "rejected", "XX"], None)
            check(client, ["SET", "created-nx", "created", "NX"], b"OK")
            check(client, ["SET", "arithmetic", "40"], b"OK")
            check(client, ["INCR", "arithmetic"], 41)
            check(client, ["INCRBY", "arithmetic", 2], 43)
            check(client, ["DECR", "arithmetic"], 42)
            check(client, ["DECRBY", "arithmetic", 2], 40)
            check(client, ["SET", "overflow", "9223372036854775807"], b"OK")
            check_error(client, ["INCR", "overflow"])
            check(client, ["SET", "persist", "permanent", "PX", 30000], b"OK")
            check(client, ["PERSIST", "persist"], 1)
            check(client, ["PERSIST", "persist"], 0)
            check(client, ["SET", "mset-ttl", "old", "PX", 30000], b"OK")
            check(client, ["MSET", "mset-ttl", "no-ttl"], b"OK")
            check(client, ["LPUSH", list_key, b"middle\x00\xff\r\n", b"left", b""], 3)
            check(client, ["RPUSH", list_key, b"right", b"\x00popped\xff"], 5)
            check(client, ["LPOP", list_key, 2], [b"", b"left"])
            check(client, ["RPOP", list_key, 1], [b"\x00popped\xff"])
            check(client, ["RPUSH", list_key, b""], 3)
            check(client, ["RPUSH", "popped-empty", "one", "two"], 2)
            check(client, ["LPOP", "popped-empty", 99], [b"one", b"two"])
            verify(client)
            server.stop(signal.SIGINT)
            server.start()
            verify(server.connect())
            passed("default strict SIGINT restart: binary/empty keys and values, list/string overwrites, atomic MSET/DEL, conditionals, arithmetic/errors, PERSIST, ordered multi-count pops")
            client = server.connect()
            check(client, ["SET", "after-orderly", "acknowledged"], b"OK")
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            verify(client)
            check(client, ["GET", "after-orderly"], b"acknowledged")
        passed("default strict SIGKILL reopen: acknowledged strings/lists survive and removed list items never reappear")


def ttl_history(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-ttl-") as directory:
        with Server(binary, directory) as server:
            client = server.connect()
            absolute = current_ms() + 30000
            check(client, ["SET", "absolute:string", "value", "PXAT", absolute], b"OK")
            check(client, ["RPUSH", "absolute:list", "left", "right"], 2)
            check(client, ["PEXPIREAT", "absolute:list", absolute], 1)
            check(client, ["LPUSH", "absolute:list", "front"], 3)
            check(client, ["RPOP", "absolute:list"], b"right")
            check(client, ["SET", "absolute:arithmetic", "10", "PXAT", absolute], b"OK")
            check(client, ["INCR", "absolute:arithmetic"], 11)
            check(client, ["SET", "absolute:arithmetic", "12", "GET", "KEEPTTL"], b"11")
            check(client, ["SET", "already-expired", "ghost", "PXAT", 1], b"OK")
            check(client, ["RPUSH", "already-expired", "new-list"], 1)
            check(client, ["RPUSH", "immediate-list", "old"], 1)
            check(client, ["PEXPIREAT", "immediate-list", 1], 1)
            check(client, ["RPUSH", "immediate-list", "new"], 1)
            check(client, ["RPUSH", "recreated-list", "must-not-return"], 1)
            check(client, ["SET", "expired-string-to-list", "must-not-return"], b"OK")
            recreate_at = current_ms() + 120
            check(client, ["PEXPIREAT", "recreated-list", recreate_at], 1)
            check(client, ["PEXPIREAT", "expired-string-to-list", recreate_at], 1)
            # Do not read the expired keys before pushing. Replay itself must
            # discard their old value at the recorded push timestamp.
            wait_until_ms(recreate_at + 30)
            check(client, ["RPUSH", "recreated-list", "fresh"], 1)
            check(client, ["LPUSH", "expired-string-to-list", "fresh"], 1)
            offline_at = current_ms() + 1000
            check(client, ["SET", "offline:string", "old", "PXAT", offline_at], b"OK")
            check(client, ["RPUSH", "offline:list", "old"], 1)
            check(client, ["PEXPIREAT", "offline:list", offline_at], 1)
            assert current_ms() < offline_at, "offline TTL expired before shutdown began"
            server.stop(signal.SIGINT)
            wait_until_ms(offline_at + 30)
            server.start()
            client = server.connect()
            check(client, ["GET", "absolute:string"], b"value")
            check(client, ["GET", "absolute:arithmetic"], b"12")
            check(client, ["LRANGE", "absolute:list", 0, -1], [b"front", b"left"])
            for key in ("absolute:string", "absolute:arithmetic", "absolute:list"):
                check_deadline(client, key, absolute)
            for key, expected in (
                ("already-expired", [b"new-list"]), ("immediate-list", [b"new"]),
                ("recreated-list", [b"fresh"]), ("expired-string-to-list", [b"fresh"]),
            ):
                check(client, ["LRANGE", key, 0, -1], expected)
                check(client, ["PTTL", key], -1)
            for key in ("offline:string", "offline:list"):
                check(client, ["TYPE", key], b"none")
                check(client, ["PTTL", key], -2)
        passed("absolute TTL replay: string/list deadlines, KEEPTTL/arithmetic/list TTL preservation, already-expired and historically expired list recreation, expiration while offline")


def blocking_delivery(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-blocking-") as directory:
        with Server(binary, directory) as server:
            control = server.connect()
            # Reverse creation order to distinguish BRPOP argument priority
            # from either creation order or replay order.
            check(control, ["RPUSH", "priority:second", "s-left", "s-right"], 2)
            check(control, ["RPUSH", "priority:first", "f-left", "f-right"], 2)
            check(control, ["BRPOP", "priority:first", "priority:second", 0], [b"priority:first", b"f-right"])
            waiter = server.connect()
            begin_wait(waiter, ["BRPOP", "delivered", 0], control)
            check(control, ["RPUSH", "delivered", "left", "delivered-right"], 2)
            actual = waiter.read()
            assert actual == [b"delivered", b"delivered-right"], actual
            # No extra command/barrier after the consumer reply: its pop must
            # already be durable when the process is killed here.
            server.stop(signal.SIGKILL)
            server.start()
            control = server.connect()
            check(control, ["LRANGE", "delivered", 0, -1], [b"left"])
            check(control, ["BRPOP", "priority:first", "priority:second", 0], [b"priority:first", b"f-left"])
            check(control, ["LRANGE", "priority:second", 0, -1], [b"s-left", b"s-right"])
            pending = server.connect()
            begin_wait(pending, ["BRPOP", "pending-only", 0], control)
            server.stop(signal.SIGKILL)
            server.start()
            control = server.connect()
            check(control, ["LRANGE", "delivered", 0, -1], [b"left"])
            check(control, ["TYPE", "priority:first"], b"none")
            check(control, ["BRPOP", "priority:first", "priority:second", 0], [b"priority:second", b"s-right"])
            check(control, ["RPUSH", "pending-only", "not-stolen"], 1)
            check(control, ["LRANGE", "pending-only", 0, -1], [b"not-stolen"])
        passed("strict blocking recovery: BRPOP delivery durable before reply, key-argument/list order retained, popped values absent, unfulfilled waiters not restored after SIGKILL")


def directory_lock(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-lock-") as directory:
        with Server(binary, directory) as server:
            control = server.connect()
            check(control, ["SET", "lock-prefix", "before"], b"OK")
            # Both processes request independent ephemeral listening ports, so
            # the rejection cannot be attributed to an occupied TCP port.
            startup_failure(binary, directory)
            check(control, ["PING"], b"PONG")
            check(control, ["SET", "lock-prefix", "after-rejected-owner"], b"OK")
            server.stop(signal.SIGKILL)
            server.start()
            check(server.connect(), ["GET", "lock-prefix"], b"after-rejected-owner")
        passed("exclusive data-directory lock rejects a second process on a different port, keeps the owner functional, and releases after SIGKILL")


def everysec_restart(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-everysec-") as directory:
        with Server(binary, directory, durability="everysec") as server:
            check(server.connect(), ["MSET", "every:a", "one", "every:b", "two"], b"OK")
            server.stop(signal.SIGINT)
            server.start()
            client = server.connect()
            check(client, ["MGET", "every:a", "every:b"], [b"one", b"two"])
            check(client, ["SET", "idle", "acknowledged-before-idle"], b"OK")
            # No commands during this interval: the actor's one-second timer,
            # not another command or shutdown, must service the dirty WAL.
            idle_until = time.monotonic() + 1.25
            while time.monotonic() < idle_until:
                time.sleep(max(0, idle_until - time.monotonic()))
            assert server.process.poll() is None, "everysec server exited during idle timer interval"
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            check(client, ["MGET", "every:a", "every:b", "idle"], [b"one", b"two", b"acknowledged-before-idle"])
        passed("everysec orderly restart and idle periodic-sync cadence followed by SIGKILL/reopen (process-crash exercise, not power-loss proof)")


def frame_offsets(data):
    """Read only physical framing; mutation payloads remain opaque."""
    assert data.startswith(FILE_MAGIC), ("unexpected WAL preamble", data[:8])
    records = []
    offset = len(FILE_MAGIC)
    while offset < len(data):
        assert len(data) - offset >= HEADER_SIZE, ("fixture has incomplete header", offset)
        assert data[offset:offset + 4] == RECORD_MAGIC, ("unexpected record magic", offset)
        length = struct.unpack_from("<I", data, offset + 4)[0]
        sequence = struct.unpack_from("<Q", data, offset + 8)[0]
        assert sequence == len(records) + 1, ("unexpected fixture sequence", sequence)
        assert 0 < length <= 16 * 1024 * 1024, ("unexpected payload size", length)
        payload = offset + HEADER_SIZE
        end = payload + length
        assert end <= len(data), ("fixture has incomplete payload", offset, length)
        records.append((offset, payload, end))
        offset = end
    assert records, "fixture contains no records"
    return records


PREFIX_VALUES = [b"prefix", b"old-a", b"old-b", None, None]
FINAL_VALUES = [b"prefix", b"new-a:" + b"a" * 4096, b"new-b:" + b"b" * 4096, b"fresh-a", b"fresh-b"]
FIXTURE_KEYS = ["keep", "pair:a", "pair:b", "fresh:a", "fresh:b"]


def make_fixture(binary, root):
    source = Path(root) / "source"
    with Server(binary, source, durability="strict") as server:
        client = server.connect()
        check(client, ["MSET", "keep", "prefix", "pair:a", "old-a", "pair:b", "old-b"], b"OK")
        check(client, ["MSET", "pair:a", FINAL_VALUES[1], "pair:b", FINAL_VALUES[2],
                       "fresh:a", "fresh-a", "fresh:b", "fresh-b"], b"OK")
    original = (source / WAL_NAME).read_bytes()
    records = frame_offsets(original)
    assert len(records) == 2, ("fixture must contain exactly two atomic MSET records", len(records))
    damaged = Path(root) / "damaged-copy"
    damaged.mkdir()
    shutil.copyfile(source / WAL_NAME, damaged / WAL_NAME)
    return source, damaged, original, records


def append_and_reopen(server, expected):
    client = server.connect()
    check(client, ["MGET", *FIXTURE_KEYS], expected)
    check(client, ["MSET", "after:a", "append-a", "after:b", "append-b"], b"OK")
    server.stop(signal.SIGINT)
    server.start()
    client = server.connect()
    check(client, ["MGET", *FIXTURE_KEYS], expected)
    check(client, ["MGET", "after:a", "after:b"], [b"append-a", b"append-b"])


def incomplete_header(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-header-tail-") as root:
        source, damaged, original, records = make_fixture(binary, root)
        header = records[-1][0]
        path = damaged / WAL_NAME
        # A deliberately incomplete prefix of a 24-byte record header at EOF.
        with path.open("ab") as wal:
            wal.write(original[header:header + 13])
        with Server(binary, damaged) as server:
            assert path.read_bytes() == original, "partial final header was not truncated exactly"
            append_and_reopen(server, FINAL_VALUES)
        assert (source / WAL_NAME).read_bytes() == original, "owned source fixture was modified"
        passed("incomplete 24-byte-header prefix at EOF recovers the complete prefix, truncates only the tail, and permits append/restart")


def incomplete_atomic_payload(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-payload-tail-") as root:
        source, damaged, original, records = make_fixture(binary, root)
        header, payload, end = records[-1]
        # Destroy the final transaction after most of its payload. Neither old
        # pair may change and neither fresh pair may leak from that transaction.
        cut = payload + (end - payload) * 3 // 4
        assert payload < cut < end
        path = damaged / WAL_NAME
        path.write_bytes(original[:cut])
        with Server(binary, damaged) as server:
            assert path.read_bytes() == original[:header], "partial payload was not removed as one whole record"
            append_and_reopen(server, PREFIX_VALUES)
        assert (source / WAL_NAME).read_bytes() == original, "owned source fixture was modified"
        passed("torn final MSET payload recovers only the prior transaction, never partial pairs, and permits append/restart")


def corrupt_complete_record(binary, header_length=False):
    label = "header-length" if header_length else "payload"
    with tempfile.TemporaryDirectory(prefix=f"ffkv-durable-corrupt-{label}-") as root:
        source, damaged, original, records = make_fixture(binary, root)
        header, payload, end = records[-1]
        corrupted = bytearray(original)
        # The header protects its length with its own CRC32C. A changed length
        # must not be mistaken for an otherwise valid incomplete payload.
        position = header + 4 if header_length else end - 1
        assert header_length or payload <= position < end
        corrupted[position] ^= 1
        corrupted = bytes(corrupted)
        path = damaged / WAL_NAME
        path.write_bytes(corrupted)
        startup_failure(binary, damaged)
        assert path.read_bytes() == corrupted, (label, "rejected WAL was modified")
        assert (source / WAL_NAME).read_bytes() == original, "owned source fixture was modified"
        passed(f"complete {label} byte flip rejects startup without truncating or changing WAL bytes")


def write_failure(binary):
    if (os.name != "posix" or resource is None or not hasattr(resource, "RLIMIT_FSIZE")
            or not hasattr(signal, "SIGXFSZ")):
        print("SKIP write-failure fail-stop: POSIX RLIMIT_FSIZE and SIGXFSZ are unavailable", flush=True)
        return
    with tempfile.TemporaryDirectory(prefix="ffkv-durable-write-failure-") as directory:
        with Server(binary, directory) as server:
            check(server.connect(), ["SET", "prior-prefix", "acknowledged"], b"OK")
        wal = Path(directory) / WAL_NAME
        prefix_size = wal.stat().st_size
        limit = prefix_size + 128
        _, hard_limit = resource.getrlimit(resource.RLIMIT_FSIZE)
        if hard_limit != resource.RLIM_INFINITY and hard_limit < limit:
            print("SKIP write-failure fail-stop: inherited hard RLIMIT_FSIZE is too small for fixture", flush=True)
            return
        with Server(binary, directory, file_limit=limit) as server:
            client = server.connect()
            check(client, ["GET", "prior-prefix"], b"acknowledged")
            try:
                result = client.command("SET", "failed-write", b"x" * (1024 * 1024))
            except (OSError, AssertionError):
                # EOF/reset is expected. A socket timeout is not sufficient:
                # expect_failure below still requires a bounded nonzero exit.
                pass
            else:
                assert isinstance(result, tuple) and result[0] == "error", (
                    "failed WAL write received a success reply", result,
                )
            server.expect_failure()
        with Server(binary, directory) as server:
            client = server.connect()
            check(client, ["GET", "prior-prefix"], b"acknowledged")
            check(client, ["GET", "failed-write"], None)
            check(client, ["SET", "after-failure", "usable"], b"OK")
            server.stop(signal.SIGINT)
            server.start()
            check(server.connect(), ["MGET", "prior-prefix", "failed-write", "after-failure"],
                  [b"acknowledged", None, b"usable"])
        passed("real RLIMIT_FSIZE write failure with SIGXFSZ ignored: no success reply, nonzero fail-stop, acknowledged prefix survives, recovery accepts subsequent writes")


def memory_contrast(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-memory-contrast-"):
        with Server(binary) as server:
            client = server.connect()
            check(client, ["SET", "memory-only", "not-durable"], b"OK")
            check(client, ["RPUSH", "memory-list", "not-durable"], 1)
            server.stop(signal.SIGINT)
            server.start()
            client = server.connect()
            check(client, ["GET", "memory-only"], None)
            check(client, ["TYPE", "memory-list"], b"none")
        passed("explicit no-data-dir contrast: a fresh in-memory process loses strings and lists")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    options = parser.parse_args()
    strings_and_lists(options.binary)
    ttl_history(options.binary)
    blocking_delivery(options.binary)
    directory_lock(options.binary)
    everysec_restart(options.binary)
    incomplete_header(options.binary)
    incomplete_atomic_payload(options.binary)
    corrupt_complete_record(options.binary)
    corrupt_complete_record(options.binary, header_length=True)
    write_failure(options.binary)
    memory_contrast(options.binary)
    passed("durability smoke complete; all processes/sockets and owned fixtures cleaned up (no power-loss or HA claim)")


if __name__ == "__main__":
    main()
