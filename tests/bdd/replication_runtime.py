"""Scenario-owned native replication transports; no development services are used."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
import select
import socket
import threading
import time

from runtime import assert_equal, is_error

TIMEOUT = 10.0
MAX_WIRE = 64 * 1024 * 1024
MAX_LINE = 4096


def eventually(observe, accept, description, timeout=TIMEOUT):
    deadline = time.monotonic() + timeout
    wake = threading.Event()
    last = None
    while True:
        last = observe()
        if accept(last):
            return last
        remaining = deadline - time.monotonic()
        assert remaining > 0, (description, last)
        wake.wait(min(0.01, remaining))


def info(node):
    reply = node.execute(["INFO", "replication"], "__replication_observer")
    assert isinstance(reply, str), reply
    return dict(line.split(":", 1) for line in reply.splitlines() if ":" in line)


def encode(arguments):
    args = [arg if isinstance(arg, bytes) else str(arg).encode() for arg in arguments]
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n" % len(arg) + arg + b"\r\n" for arg in args)


def frame(data, start=0):
    """Decode only bounded command arrays, preserving the original byte span."""
    def line(position):
        end = data.find(b"\r\n", position)
        if end < 0:
            assert len(data) - position <= MAX_LINE, "RESP line bound exceeded"
            return None
        assert end - position <= MAX_LINE, "RESP line bound exceeded"
        return bytes(data[position:end]), end + 2

    head = line(start)
    if head is None:
        return None
    text, position = head
    assert text.startswith(b"*"), ("expected command array", text[:100])
    count = int(text[1:])
    assert 0 < count <= 100000, count
    args = []
    for _ in range(count):
        head = line(position)
        if head is None:
            return None
        text, position = head
        assert text.startswith(b"$"), text[:100]
        length = int(text[1:])
        assert 0 <= length <= MAX_WIRE, length
        if len(data) < position + length + 2:
            return None
        assert data[position + length:position + length + 2] == b"\r\n"
        args.append(bytes(data[position:position + length]))
        position += length + 2
        assert position - start <= MAX_WIRE, "command bound exceeded"
    return args, position


class Wire:
    def __init__(self, connection):
        self.socket = connection
        self.buffer = bytearray()

    def more(self, deadline):
        remaining = deadline - time.monotonic()
        assert remaining > 0, "replication wire deadline exceeded"
        assert select.select([self.socket], [], [], remaining)[0], "replication wire read timeout"
        data = self.socket.recv(65536)
        assert data, "unexpected replication wire EOF"
        self.buffer.extend(data)
        assert len(self.buffer) <= MAX_WIRE, "replication wire buffer bound exceeded"

    def take(self, length, deadline):
        assert 0 <= length <= MAX_WIRE, length
        while len(self.buffer) < length:
            self.more(deadline)
        result = bytes(self.buffer[:length])
        del self.buffer[:length]
        return result

    def line(self, deadline):
        while True:
            end = self.buffer.find(b"\r\n")
            if end >= 0:
                assert end <= MAX_LINE
                return self.take(end + 2, deadline)
            assert len(self.buffer) <= MAX_LINE
            self.more(deadline)

    def command(self, deadline):
        while True:
            result = frame(self.buffer)
            if result is not None:
                args, end = result
                return args, self.take(end, deadline)
            self.more(deadline)


def validate_rdb(data):
    assert 18 <= len(data) <= MAX_WIRE, len(data)
    assert data[:5] == b"REDIS" and data[5:9].isdigit(), data[:9]
    assert 1 <= int(data[5:9]) <= 99, data[:9]
    assert data[-9] == 255, "missing RDB EOF opcode"
    # Redis's reflected CRC64/Jones polynomial, independent of ffkv's encoder.
    crc = 0
    for byte in data[:-8]:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x95AC9329AC4BC9B5 if crc & 1 else 0)
    checksum = int.from_bytes(data[-8:], "little")
    assert checksum != 0, "probe requested a checksummed snapshot"
    assert crc == checksum, ("Redis RDB CRC64 mismatch", crc, checksum)


def snapshot(wire, deadline, first=None):
    line = first or wire.line(deadline)
    while line == b"\r\n":
        line = wire.line(deadline)
    assert line.startswith(b"$"), ("expected RDB transfer", line)
    if line.startswith(b"$EOF:"):
        marker = line[5:-2]
        assert len(marker) == 40
        data = bytearray()
        while True:
            end = wire.buffer.find(marker)
            if end >= 0:
                data.extend(wire.take(end, deadline))
                wire.take(len(marker), deadline)
                break
            if len(wire.buffer) > len(marker):
                data.extend(wire.take(len(wire.buffer) - len(marker), deadline))
            assert len(data) <= MAX_WIRE, "RDB transfer bound exceeded"
            wire.more(deadline)
        data = bytes(data)
    else:
        length = int(line[1:-2])
        assert 18 <= length <= MAX_WIRE, length
        data = wire.take(length, deadline)
    validate_rdb(data)
    return data


@dataclass
class Session:
    request: bytearray = field(default_factory=bytearray)
    response: bytearray = field(default_factory=bytearray)
    delivered: int = 0


class ReplicationProxy:
    """One directional gate with an explicit worker barrier and bounded capture."""
    def __init__(self, target):
        self.target = target
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(16)
        self.address = self.listener.getsockname()
        self.wake_read, self.wake_write = socket.socketpair()
        self.condition = threading.Condition()
        self.enabled = True
        self.paused = False
        self.stopped = False
        self.revision = 0
        self.observed_revision = 0
        self.sessions = []
        self.accepted = 0
        self.error = None
        self.thread = threading.Thread(target=self._run, name="bdd-replication-proxy", daemon=True)
        self.thread.start()

    def change(self, *, enabled=None, paused=None):
        with self.condition:
            if enabled is not None:
                self.enabled = enabled
            if paused is not None:
                self.paused = paused
            self.revision += 1
            wanted = self.revision
            self.wake_write.send(b"x")
            assert self.condition.wait_for(lambda: self.observed_revision >= wanted or self.error is not None, TIMEOUT), "proxy control barrier timeout"
            self.check()

    def check(self):
        assert self.error is None, ("owned proxy failed", self.error)

    def _run(self):
        pairs = []
        try:
            while True:
                with self.condition:
                    if not self.enabled or self.stopped:
                        for client, upstream, _, _, _ in pairs:
                            client.close()
                            upstream.close()
                        pairs.clear()
                    self.observed_revision = self.revision
                    self.condition.notify_all()
                    if self.stopped:
                        return
                    paused = self.paused
                reads = [self.listener, self.wake_read]
                writes = []
                for client, upstream, outbound, inbound, session in pairs:
                    if len(outbound) < MAX_WIRE:
                        reads.append(client)
                    if len(inbound) < MAX_WIRE:
                        reads.append(upstream)
                    if outbound:
                        writes.append(upstream)
                    if inbound and not paused:
                        writes.append(client)
                readable, writable, _ = select.select(reads, writes, [], TIMEOUT)
                if self.wake_read in readable:
                    self.wake_read.recv(65536)
                    # Rebuild readiness after acknowledging changed gate state.
                    continue
                if self.listener in readable:
                    client, _ = self.listener.accept()
                    with self.condition:
                        self.accepted += 1
                        enabled = self.enabled
                    if not enabled:
                        client.close()
                    else:
                        try:
                            upstream = socket.create_connection(self.target, timeout=TIMEOUT)
                        except OSError:
                            client.close()
                            continue
                        client.setblocking(False)
                        upstream.setblocking(False)
                        session = Session()
                        with self.condition:
                            self.sessions.append(session)
                        pairs.append((client, upstream, bytearray(), bytearray(), session))
                for pair in list(pairs):
                    client, upstream, outbound, inbound, session = pair
                    closed = False
                    for source, destination, pending, capture in (
                        (client, upstream, outbound, session.request),
                        (upstream, client, inbound, session.response),
                    ):
                        if source in readable:
                            try:
                                chunk = source.recv(65536)
                            except (ConnectionError, OSError):
                                chunk = b""
                            if not chunk:
                                closed = True
                                break
                            with self.condition:
                                capture.extend(chunk)
                                assert len(capture) <= MAX_WIRE, "proxy capture bound exceeded"
                                self.condition.notify_all()
                            pending.extend(chunk)
                        if destination in writable and pending:
                            try:
                                sent = destination.send(pending)
                            except (ConnectionError, OSError):
                                closed = True
                                break
                            del pending[:sent]
                            if destination is client:
                                with self.condition:
                                    session.delivered += sent
                    if closed:
                        client.close()
                        upstream.close()
                        pairs.remove(pair)
        except BaseException as error:
            with self.condition:
                self.error = repr(error)
                self.condition.notify_all()
        finally:
            for client, upstream, _, _, _ in pairs:
                client.close()
                upstream.close()

    def captures(self):
        with self.condition:
            self.check()
            return [(bytes(s.request), bytes(s.response), s.delivered) for s in self.sessions]

    def close(self):
        with self.condition:
            self.stopped = True
        try:
            self.wake_write.send(b"x")
        except OSError:
            pass
        self.thread.join(TIMEOUT)
        for resource in (self.listener, self.wake_read, self.wake_write):
            resource.close()
        assert not self.thread.is_alive(), "owned proxy thread did not stop"


def sync_capture(request, response):
    position = 0
    command = None
    while position < len(request):
        result = frame(request, position)
        if result is None:
            break
        args, position = result
        if args[0].upper() in (b"PSYNC", b"SYNC"):
            command = args
            break
    if command is None:
        return None
    position = 0
    while True:
        end = response.find(b"\r\n", position)
        if end < 0:
            return None
        line = response[position:end]
        position = end + 2
        if line.startswith(b"+FULLRESYNC "):
            return command, "full", line, position
        if line.startswith(b"+CONTINUE"):
            return command, "partial", line, position
        assert line.startswith(b"+"), ("replication handshake error", line)


class SnapshotReplay:
    """Serve bytes captured from the real source to a real owned native follower.

    Only handshake replies are fixture-generated. Snapshot, live effects, ACK and
    FACK bytes come from the actual source/follower; no durability is invented.
    """
    def __init__(self, probe):
        self.probe = probe
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.listener.settimeout(TIMEOUT)
        self.address = self.listener.getsockname()
        self.wake_read, self.wake_write = socket.socketpair()
        self.connection = None
        self.stopped = threading.Event()
        self.ready = threading.Event()
        self.send_lock = threading.Lock()
        self.error = None
        self.acknowledgements = []
        self.thread = threading.Thread(target=self._run, name="bdd-rdb-replay", daemon=True)
        self.thread.start()

    def _run(self):
        try:
            readable, _, _ = select.select([self.listener, self.wake_read], [], [], TIMEOUT)
            if self.stopped.is_set():
                return
            assert self.listener in readable, "native snapshot replay accept timeout"
            self.connection, _ = self.listener.accept()
            if self.stopped.is_set():
                self.connection.close()
                return
            self.connection.settimeout(TIMEOUT)
            wire = Wire(self.connection)
            deadline = time.monotonic() + TIMEOUT
            while True:
                args, _ = wire.command(deadline)
                if args[0].upper() == b"PSYNC":
                    break
                assert args[0].upper() in (b"PING", b"REPLCONF"), args
                self.connection.sendall(b"+PONG\r\n" if args[0].upper() == b"PING" else b"+OK\r\n")
            with self.send_lock:
                self.connection.sendall(b"+FULLRESYNC " + self.probe.history + b" " + str(self.probe.base_offset).encode() + b"\r\n")
                self.connection.sendall(b"$%d\r\n" % len(self.probe.rdb) + self.probe.rdb)
            self.ready.set()
            while not self.stopped.is_set():
                args, raw = wire.command(time.monotonic() + TIMEOUT)
                assert len(args) in (3, 5) and args[:2] == [b"REPLCONF", b"ACK"], args
                processed = int(args[2])
                assert processed <= self.probe.offset, (processed, self.probe.offset)
                if len(args) == 5:
                    assert args[3] == b"FACK" and int(args[4]) <= processed, args
                self.acknowledgements.append(args)
                # SYNC exposes no source offset: its local replay origin must
                # never be presented as an acknowledgement of primary history.
                if not self.probe.legacy:
                    with self.probe.send_lock:
                        self.probe.socket.sendall(raw)
        except BaseException as error:
            if not self.stopped.is_set():
                self.error = repr(error)
                self.ready.set()

    def send(self, raw):
        assert self.ready.wait(TIMEOUT), "snapshot replay handshake timeout"
        assert self.error is None, self.error
        with self.send_lock:
            self.connection.sendall(raw)

    def close(self):
        self.stopped.set()
        self.wake_write.send(b"x")
        self.listener.close()
        if self.connection is not None:
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.connection.close()
        self.thread.join(TIMEOUT)
        self.wake_read.close()
        self.wake_write.close()
        assert not self.thread.is_alive(), "snapshot replay thread did not stop"


class NativeProbe:
    def __init__(self, fixture, name, node="master", legacy=False, unknown=False, install=True):
        self.fixture, self.name, self.node_name = fixture, name, node
        self.legacy = legacy
        self.socket = None
        self.replay = None
        self.apply_node = None
        self.send_lock = threading.Lock()
        self.raw_stream = bytearray()
        fixture.root.cleanups.append(self.close)
        source = fixture.root.node(node)
        self.snapshot_key = "__bdd_probe_snapshot_" + name
        self.snapshot_value = "captured-generation-" + str(fixture.sequence)
        assert fixture.master.execute(["SET", self.snapshot_key, self.snapshot_value], "__probe_snapshot_seed") == "OK"
        eventually(lambda: source.execute(["GET", self.snapshot_key], "__probe_snapshot_observer"), lambda value: value == self.snapshot_value, "probe source did not reach snapshot sentinel")
        status = info(source)
        self.history = status["master_replid"].encode()
        self.base_offset = int(status["master_repl_offset"])
        self.socket = socket.create_connection(source.address, timeout=TIMEOUT)
        self.socket.settimeout(TIMEOUT)
        self.wire = Wire(self.socket)
        deadline = time.monotonic() + TIMEOUT
        for args in (["PING"], ["REPLCONF", "listening-port", "0"], ["REPLCONF", "capa", "psync2"]):
            self.socket.sendall(encode(args))
            expected = b"+PONG\r\n" if args[0] == "PING" else b"+OK\r\n"
            assert self.wire.line(deadline) == expected, args
            fixture.root.evidence.positive(args[0])
        if legacy:
            request = ["SYNC"]
        elif unknown:
            history = "0" * 40 if self.history != b"0" * 40 else "1" * 40
            request = ["PSYNC", history, self.base_offset + 1]
        else:
            request = ["PSYNC", "?", "-1"]
        self.socket.sendall(encode(request))
        first = self.wire.line(deadline)
        self.header = None
        if not legacy:
            assert first.startswith(b"+FULLRESYNC "), first
            self.header = first
            _, self.history, offset = first.strip().split()
            assert len(self.history) == 40
            self.base_offset = int(offset)
            first = None
        else:
            assert first.startswith(b"$"), ("SYNC unexpectedly returned a full-resync header", first)
        self.rdb = snapshot(self.wire, deadline, first)
        if legacy:
            # This distinct replay history starts at zero. The real SYNC source
            # supplied no offset, so it receives no fabricated ACK/FACK.
            self.history = hashlib.sha256(self.rdb).hexdigest()[:40].encode()
            self.base_offset = 0
        self.offset = self.base_offset
        self.write_offset = self.base_offset
        if install:
            self.replay = SnapshotReplay(self)
            self.apply_node = fixture.root.add_node("probe-" + name + "-" + str(fixture.sequence), extra_args=["--native-replication-allow-loopback"])
            fixture.sequence += 1
            reply = self.apply_node.execute(["REPLICAOF", self.replay.address[0], self.replay.address[1]])
            assert not is_error(reply), reply
            eventually(lambda: info(self.apply_node), lambda value: value.get("master_link_status") == "up" and int(value.get("slave_repl_offset", "-1")) == self.base_offset, "captured RDB was not installed by native follower")
            assert self.apply_node.execute(["GET", self.snapshot_key], "__probe_snapshot_observer") == self.snapshot_value, "native follower did not install captured RDB data"
            assert self.replay.error is None, self.replay.error
            # A FULLRESYNC header alone is not command success: the bounded,
            # validated RDB must actually install its sentinel in a real follower.
            fixture.root.evidence.positive("SYNC" if legacy else "PSYNC")

    def pump(self, deadline):
        args, raw = self.wire.command(deadline)
        self.offset += len(raw)
        if args[0].upper() not in (b"PING", b"REPLCONF", b"SELECT", b"MULTI"):
            self.write_offset = self.offset
        self.raw_stream.extend(raw)
        assert len(self.raw_stream) <= MAX_WIRE, "probe history bound exceeded"
        # A decoded live frame from the real source is positive stream evidence,
        # including probes intentionally not replayed into another server.
        self.fixture.root.evidence.positive("SYNC" if self.legacy else "PSYNC")
        if self.replay is not None:
            self.replay.send(raw)
        return args

    def through_offset(self, offset):
        deadline = time.monotonic() + TIMEOUT
        while self.offset < offset:
            self.pump(deadline)
        assert self.offset == offset, ("offset did not end at RESP frame boundary", self.offset, offset)

    def through_value(self, key, value):
        deadline = time.monotonic() + TIMEOUT
        while True:
            assert self.replay is not None and self.replay.error is None, getattr(self.replay, "error", None)
            result = self.apply_node.execute(["GET", key], "__probe_observer")
            if result == value:
                return
            self.pump(deadline)

    def close(self):
        if self.replay is not None:
            self.replay.close()
            self.replay = None
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.socket.close()
            self.socket = None


def dataset(node):
    """Compare actual serialized values in every Redis logical database."""
    result = []
    client = "__dataset_observer"
    database_count = node.execute(["CONFIG", "GET", "databases"], client)
    if isinstance(database_count, dict):
        count = int(database_count["databases"])
    else:
        assert isinstance(database_count, list) and len(database_count) == 2, database_count
        count = int(database_count[1])
    assert 0 < count <= 1024, count
    for database in range(count):
        assert node.execute(["SELECT", database], client) == "OK"
        keys = node.execute(["KEYS", "*"], client)
        assert isinstance(keys, list), keys
        for key in sorted(keys, key=lambda value: repr(value)):
            argument = {"base64": key["base64"]} if isinstance(key, dict) and key.get("type") == "bytes" else key
            result.append((database, key, node.execute(["DUMP", argument], client), node.execute(["PEXPIRETIME", argument], client)))
    assert node.execute(["SELECT", 0], client) == "OK"
    return result


class ReplicationFixture:
    def __init__(self, root, *, synchronized=False, backlog=None, protected=False, aof_disabled=False):
        self.root = root
        self.sequence = 0
        self.probes = {}
        self.history_probe = None
        self.sync_index = 0
        self.reconnect_target = None
        self.gate = root.directory / "replica-fsync.closed"
        master_args = ["--native-replication-allow-loopback"]
        if backlog is not None:
            master_args += ["--replication-backlog-bytes", str(backlog)]
        if aof_disabled:
            master_args += ["--appendonly", "false"]
        self.master = root.add_node("master", extra_args=master_args)
        replica_args = ["--replication-fsync-gate", str(self.gate)]
        if not protected:
            replica_args += ["--native-replication-allow-loopback"]
        else:
            credential_file = root.directory / "protected-upstream.password"
            credential_file.write_text("fixture-upstream-secret\n")
            credential_file.chmod(0o600)
            replica_args += ["--native-replica-user", "fixture-upstream-user", "--native-replica-password-file", str(credential_file)]
        self.replica = root.add_node("replica", extra_args=replica_args)
        self.proxy = ReplicationProxy(self.master.address)
        root.cleanups.append(self.proxy.close)
        root.cleanups.append(self.open_gate)
        root.placeholders["replication_host"] = self.proxy.address[0]
        root.placeholders["replication_port"] = str(self.proxy.address[1])
        # Ordinary REPLICAOF in the feature must traverse the owned fault link;
        # explicit FAILOVER target continues to use the real replica endpoint.
        root.placeholders["master_host"] = self.proxy.address[0]
        root.placeholders["master_port"] = str(self.proxy.address[1])
        self.before_dataset = None
        if synchronized:
            self.follow()

    def follow(self):
        reply = self.replica.execute(["REPLICAOF", self.proxy.address[0], self.proxy.address[1]])
        assert not is_error(reply), reply
        eventually(lambda: info(self.replica), lambda value: value.get("master_link_status") == "up", "native pair did not synchronize")
        self.master.execute(["SET", "__bdd_sync_barrier", "installed"], "__pair_barrier")
        eventually(lambda: self.replica.execute(["GET", "__bdd_sync_barrier"], "__pair_barrier"), lambda value: value == "installed", "native live apply barrier")
        assert self.master.execute(["WAIT", 1, 2000], "__pair_barrier") == 1
        self.master.execute(["DEL", "__bdd_sync_barrier"], "__pair_barrier")
        assert self.master.execute(["WAIT", 1, 2000], "__pair_barrier") == 1

    def disconnect(self):
        # A non-acking observer captures the independent primary history, and
        # cannot accidentally satisfy WAIT/WAITAOF for the disconnected replica.
        self.history_probe = NativeProbe(self, "history-" + str(self.sequence), install=False)
        self.sequence += 1
        eventually(lambda: info(self.replica), lambda value: int(value.get("slave_repl_offset", "-1")) >= self.history_probe.base_offset, "replica did not reach the captured history origin")
        self.proxy.change(enabled=False)
        disconnected = eventually(lambda: info(self.replica), lambda value: value.get("master_link_status") == "down", "replica connection did not close")
        self.disconnect_offset = int(disconnected["slave_repl_offset"])
        self.sync_index = len(self.proxy.captures())

    def reconnect(self):
        self.reconnect_target = int(info(self.master)["master_repl_offset"])
        # Drain observer before reopening, avoiding eviction from the same bounded
        # backlog that is intentionally being exercised by the feature.
        self.history_probe.through_offset(self.reconnect_target)
        self.proxy.change(enabled=True)

    def next_sync(self, kind):
        def observe():
            for request, response, delivered in self.proxy.captures()[self.sync_index:]:
                parsed = sync_capture(request, response)
                if parsed is not None:
                    return parsed, response, delivered
            return None
        result = eventually(observe, lambda value: value is not None, "missing observed replica synchronization")
        assert result[0][1] == kind, result[0]
        self.observed_sync = result
        return result

    def assert_resumed_bytes(self):
        parsed, _, _ = self.observed_sync
        args, kind, _, stream_start = parsed
        assert kind == "partial" and args[0] == b"PSYNC", parsed
        next_offset = int(args[2])
        assert args[1] == self.history_probe.history, (args, self.history_probe.history)
        assert next_offset == self.disconnect_offset + 1, (next_offset, self.disconnect_offset)
        assert next_offset >= self.history_probe.base_offset + 1, (next_offset, self.history_probe.base_offset)
        length = self.reconnect_target - next_offset + 1
        assert length > 0, "scenario must have missing history bytes"
        expected_start = next_offset - self.history_probe.base_offset - 1
        expected = bytes(self.history_probe.raw_stream[expected_start:expected_start + length])
        assert len(expected) == length
        def observe():
            for request, response, delivered in self.proxy.captures()[self.sync_index:]:
                current = sync_capture(request, response)
                if current is not None and current[1] == "partial":
                    return response[stream_start:stream_start + length], delivered
            return b"", 0
        actual, _ = eventually(observe, lambda value: len(value[0]) == length and value[1] >= stream_start + length, "resumed bytes were not delivered")
        assert actual == expected, ("partial resync changed primary history bytes", actual, expected)
        self.root.evidence.positive("PSYNC")

    def close_gate(self):
        self.gate.write_bytes(b"closed\n")

    def open_gate(self):
        self.gate.unlink(missing_ok=True)

    def assert_gate_entered(self):
        entered = Path(str(self.gate) + ".entered")
        eventually(entered.exists, bool, "real replica WAL fsync never reached owned gate")

    def crash_replica(self):
        # Recovery must prove the replica's own durable WAL, not silently fetch
        # the asserted value again from the still-running primary.
        self.proxy.change(enabled=False)
        eventually(lambda: info(self.replica), lambda value: value.get("master_link_status") == "down", "replica upstream did not disconnect before crash")
        self.replica.restart_after_crash()

    def protected_configure(self):
        assert self.master.execute(["SET", "protected-snapshot-only", "must-not-install"]) == "OK"
        assert self.replica.execute(["SET", "protected-local-only", "must-retain"]) == "OK"
        self.before_dataset = dataset(self.replica)
        self.follow_reply = self.replica.execute(["REPLICAOF", self.proxy.address[0], self.proxy.address[1]])
        assert not is_error(self.follow_reply), self.follow_reply

    def assert_protected(self):
        eventually(lambda: info(self.replica), lambda value: int(value.get("master_connect_denied", "0")) >= 1, "protected upstream policy refusal not observed")
        self.proxy.check()
        with self.proxy.condition:
            assert self.proxy.accepted == 0, ("protected endpoint received TCP connection", self.proxy.accepted)
            assert not select.select([self.proxy.listener], [], [], 0)[0], "protected endpoint has an unaccepted TCP connection"
        assert not self.proxy.captures(), "protected upstream received credential or protocol bytes"
