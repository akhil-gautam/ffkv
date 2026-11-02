"""Owned real-network cluster topology, observations, and concurrency barriers."""
from __future__ import annotations

import binascii
import json
import os
import re
import secrets
import signal
import socket
import threading
import time

from runtime import NativeClient, assert_equal, is_error
from evidence import text_reply


SLOT_RANGES = ((0, 5460), (5461, 10922), (10923, 16383))
IDENTIFIER = re.compile(r"[0-9a-f]{40}\Z")


def key_slot(key):
    encoded = key.encode() if isinstance(key, str) else key
    opening = encoded.find(b"{")
    if opening >= 0:
        closing = encoded.find(b"}", opening + 1)
        if closing > opening + 1:
            encoded = encoded[opening + 1:closing]
    return binascii.crc_hqx(encoded, 0) % 16384


def key_for_slot(slot, suffix="key"):
    assert 0 <= slot < 16384, ("invalid cluster slot", slot)
    for candidate in range(1_000_000):
        tag = f"bdd-{candidate}"
        if key_slot(tag) == slot:
            return "{" + tag + "}:" + suffix
    raise AssertionError(("could not find CRC16 hash tag", slot))


def fields(reply):
    reply = text_reply(reply)
    return dict(line.split(":", 1) for line in reply.splitlines() if ":" in line and not line.startswith("#"))


def node_rows(reply):
    reply = text_reply(reply)
    result = {}
    for line in reply.splitlines():
        row = line.split()
        assert len(row) >= 8, ("malformed CLUSTER NODES row", line)
        result[row[0]] = {"address": row[1], "flags": set(row[2].split(",")),
                          "parent": row[3], "link": row[7], "slots": row[8:]}
    return result


class BusProxy:
    """Forward untouched owned bus frames, with explicit snapshot/mutation reply gates."""
    def __init__(self, name, listener, backend_port):
        self.name, self.listener, self.backend_port = name, listener, backend_port
        self.stopped = threading.Event()
        self.gates = {}
        self.failure = None
        self.connections = set()
        self.threads = []
        self.lock = threading.Lock()
        listener.listen(64)
        listener.settimeout(0.2)
        self.acceptor = threading.Thread(target=self.accept, name=f"cluster-bus-{name}", daemon=True)
        self.acceptor.start()

    def arm(self, kind="snapshot"):
        assert kind in ("snapshot", "mutation"), kind
        assert kind not in self.gates, (self.name, "bus stream gate already armed", kind)
        self.gates[kind] = {"paused": threading.Event(), "released": threading.Event()}

    def release(self, kind=None):
        kinds = list(self.gates) if kind is None else [kind]
        for stream in kinds:
            assert stream in self.gates, (self.name, "bus stream gate is not armed", stream)
            self.gates.pop(stream)["released"].set()

    def exact(self, connection, size):
        value = bytearray()
        while len(value) < size and not self.stopped.is_set():
            try:
                part = connection.recv(size - len(value))
            except socket.timeout:
                continue
            if not part:
                raise EOFError()
            value.extend(part)
        if len(value) != size:
            raise EOFError()
        return bytes(value)

    def frame(self, connection):
        header = self.exact(connection, 4)
        size = int.from_bytes(header, "big")
        assert 0 < size <= 16 * 1024 * 1024, (self.name, "invalid native bus frame", size)
        return header, self.exact(connection, size)

    def accept(self):
        while not self.stopped.is_set():
            try:
                incoming, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if not self.stopped.is_set():
                    self.failure = AssertionError((self.name, "bus proxy listener failed"))
                return
            with self.lock:
                self.connections.add(incoming)
                worker = threading.Thread(target=self.forward, args=(incoming,), daemon=True,
                                          name=f"cluster-bus-connection-{self.name}")
                self.threads.append(worker)
                worker.start()

    def forward(self, incoming):
        outgoing = None
        try:
            stream_kind = None
            outgoing = socket.create_connection(("127.0.0.1", self.backend_port), timeout=2)
            incoming.settimeout(0.2)
            outgoing.settimeout(0.2)
            with self.lock:
                self.connections.add(outgoing)
            while not self.stopped.is_set():
                header, request = self.frame(incoming)
                outgoing.sendall(header + request)
                header, reply = self.frame(outgoing)
                envelope = json.loads(request)
                request = envelope["request"]
                request_kind = next(iter(request)) if isinstance(request, dict) else request
                if request_kind == "Export":
                    stream_kind = "snapshot"
                elif request_kind == "Changes":
                    stream_kind = "mutation"
                elif request_kind == "Globals":
                    stream_kind = "globals"
                gate = self.gates.get(stream_kind)
                if (gate is not None and reply.startswith(b'{"Chunk":') and
                        request_kind in ("Export", "ExportContinue", "Changes")):
                    gate["paused"].set()
                    assert gate["released"].wait(30), (
                        self.name, stream_kind, "bus stream reply gate was never released")
                incoming.sendall(header + reply)
        except (EOFError, OSError):
            # Peer EOF/reset is expected during crash, cancellation, and RPC timeout.
            pass
        except BaseException as error:
            self.failure = error
            for gate in list(self.gates.values()):
                gate["paused"].set()
        finally:
            for connection in (incoming, outgoing):
                if connection is not None:
                    connection.close()
                    with self.lock:
                        self.connections.discard(connection)

    def wait_paused(self, kind="snapshot"):
        gate = self.gates.get(kind)
        assert gate is not None, (self.name, "bus stream gate is not armed", kind)
        assert gate["paused"].wait(15), (self.name, kind, "no actual stream chunk reached the bus gate")
        assert self.failure is None, (self.name, "bus proxy failed", self.failure)
        assert not gate["released"].is_set(), (self.name, kind, "bus stream gate already released")

    def close(self):
        self.release()
        self.stopped.set()
        self.listener.close()
        with self.lock:
            connections = list(self.connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.acceptor.join(timeout=3)
        for worker in self.threads:
            worker.join(timeout=3)
        assert not self.acceptor.is_alive() and all(not worker.is_alive() for worker in self.threads), (
            self.name, "bus proxy threads failed to stop")
        assert self.failure is None, (self.name, "bus proxy failed", self.failure)


class ClusterFixture:
    def __init__(self, runtime, count, assigned, metrics=False):
        assert not runtime.nodes and runtime.process is None, "cluster fixture requires a fresh scenario"
        self.runtime = runtime
        self.names = [f"node{index}" for index in range(1, count + 1)]
        self.ids = {}
        self.shard_ids = {}
        self.paused = set()
        self.writers = {}
        self.timeout = 30
        self.assigned = assigned
        self.metrics = metrics
        self.secret = secrets.token_hex(32)
        self.ports = {}
        self.reservations = {}
        self.proxies = {}
        self.config_directories = {}
        self.configuration_faults = set()
        runtime.cleanups.append(self.close)
        for name in self.names:
            reservations = []
            for _ in range(3):
                reservation = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                reservation.bind(("127.0.0.1", 0))
                reservations.append(reservation)
            self.reservations[name] = reservations
            self.ports[name] = tuple(sock.getsockname()[1] for sock in reservations)
        for name in self.names:
            self.start_node(name)
        runtime.default_node = "node1"
        self.discover()
        for name in self.names[1:]:
            self.command("node1", ["CLUSTER", "MEET", "127.0.0.1", self.ports[name][0], self.ports[name][1]])
        self.wait_membership()
        if assigned:
            for name, (start, end) in zip(self.names[:3], SLOT_RANGES):
                self.command(name, ["CLUSTER", "ADDSLOTSRANGE", start, end])
            self.wait_initial_slots()
            for replica, primary in zip(self.names[3:], self.names[:3]):
                self.command(replica, ["CLUSTER", "REPLICATE", self.ids[primary]])
            self.wait_initial_roles()
            self.discover()
        self.publish_keys()
        self.initial_slots = self.command("node1", ["CLUSTER", "SLOTS"])

    def start_node(self, name):
        port, bus_port, backend_port = self.ports[name]
        config_directory = self.runtime.directory / f"{name}-cluster"
        config_directory.mkdir(mode=0o700)
        self.config_directories[name] = config_directory
        config = config_directory / "nodes.json"
        args = self.startup_arguments(port, backend_port, bus_port, config)
        client_reservation, proxy_listener, backend_reservation = self.reservations.pop(name)
        self.proxies[name] = BusProxy(name, proxy_listener, backend_port)
        client_reservation.close()
        backend_reservation.close()
        node = self.runtime.add_node(name, extra_args=args)
        node.set_version("__cluster_observer", 3)
        assert node.address == ("127.0.0.1", port), (name, "unexpected listener", node.address, port)
        self.runtime.placeholders.update({f"{name}_host": "127.0.0.1", f"{name}_port": str(port),
                                          f"{name}_bus_port": str(bus_port)})

    def fault_configuration_directory(self, name):
        """Force real ENOTDIR on config writes, including when tests run as root."""
        self.node(name)
        assert name not in self.configuration_faults, (name, "configuration directory already faulted")
        directory = self.config_directories[name]
        backup = directory.with_name(directory.name + "-preserved")
        assert directory.is_dir() and not backup.exists(), (name, directory, backup)
        directory.rename(backup)
        self.configuration_faults.add(name)
        def restore():
            if directory.is_file():
                directory.unlink()
            backup.rename(directory)
            self.configuration_faults.remove(name)
        self.runtime.cleanups.append(restore)
        directory.write_bytes(b"owned BDD configuration directory write fault\n")

    def startup_arguments(self, port, backend_port, bus_port, config):
        args = ["--bind", f"127.0.0.1:{port}", "--cluster-enabled", "--cluster-config-file", str(config),
                "--cluster-bus-port", str(backend_port), "--cluster-announce-bus-port", str(bus_port),
                "--cluster-node-timeout-ms", "2000", "--cluster-bus-secret", self.secret,
                "--native-replication-allow-loopback",
                "--cluster-require-full-coverage", "true" if self.assigned else "false"]
        if self.metrics:
            args.extend(["--cluster-slot-stats-cpu", "--cluster-slot-stats-memory", "--cluster-slot-stats-network"])
        return args

    def node(self, name):
        assert name in self.names, ("unknown fixture node", name, self.names)
        return self.runtime.node(name)

    def query(self, name, command):
        node = self.node(name)
        assert node.process is not None and node.process.poll() is None, (name, "cluster process is not running")
        try:
            return node.client("__cluster_observer").execute(command)
        except AssertionError as error:
            raise AssertionError((name, command, "native cluster observation failed", str(error))) from error

    def command(self, name, command):
        reply = self.query(name, command)
        assert not is_error(reply), (name, command, reply)
        return reply

    def eventually(self, description, observe, accepts, timeout=None):
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        latest = None
        while True:
            latest = observe()
            if accepts(latest):
                return latest
            remaining = deadline - time.monotonic()
            assert remaining > 0, (description, "cluster consensus timeout", latest,
                                   {name: None if self.node(name).process is None else self.node(name).process.poll()
                                    for name in self.names})
            time.sleep(min(0.02, remaining))

    def discover(self):
        for name in self.names:
            node_id = self.command(name, ["CLUSTER", "MYID"])
            shard_id = self.command(name, ["CLUSTER", "MYSHARDID"])
            assert isinstance(node_id, str) and IDENTIFIER.fullmatch(node_id), (name, node_id)
            assert isinstance(shard_id, str) and IDENTIFIER.fullmatch(shard_id), (name, shard_id)
            self.ids[name], self.shard_ids[name] = node_id, shard_id
            self.runtime.placeholders.update({f"{name}_id": node_id, f"{name}_shard_id": shard_id,
                                              f"{name}_shardid": shard_id})
        assert len(set(self.ids.values())) == len(self.names), ("duplicate live cluster node IDs", self.ids)

    def publish_keys(self):
        values = self.runtime.placeholders
        for slot in (0, 1, 2, 3, 5460, 5461, 10922, 10923, 16383):
            values[f"slot{slot}_key"] = key_for_slot(slot)
            assert self.command("node1", ["CLUSTER", "KEYSLOT", values[f"slot{slot}_key"]]) == slot, (
                "server and CRC16 fixture disagree", slot, values[f"slot{slot}_key"])
        for index, (start, _) in enumerate(SLOT_RANGES, 1):
            values[f"node{index}_slot"] = str(start + 1)
            values[f"node{index}_key"] = key_for_slot(start + 1)
            assert self.command("node1", ["CLUSTER", "KEYSLOT", values[f"node{index}_key"]]) == start + 1, (
                "server and CRC16 fixture disagree", index, values[f"node{index}_key"])

    def wait_membership(self):
        expected = set(self.ids.values())
        self.eventually("all nodes discover every real peer",
                        lambda: {name: node_rows(self.command(name, ["CLUSTER", "NODES"])) for name in self.names},
                        lambda views: all(set(rows) == expected and
                                          all(row["link"] == "connected" and not ({"fail", "fail?", "handshake"} & row["flags"])
                                              for row in rows.values()) for rows in views.values()))

    def wait_initial_slots(self):
        expected = [(start, end, self.ids[name]) for name, (start, end) in zip(self.names[:3], SLOT_RANGES)]
        self.eventually("all primaries agree on initial slot ownership",
                        lambda: {name: self.command(name, ["CLUSTER", "SLOTS"]) for name in self.names},
                        lambda views: all([(row[0], row[1], row[2][2]) for row in rows] == expected
                                          for rows in views.values()))

    def wait_initial_roles(self):
        parents = {self.ids[replica]: self.ids[primary] for replica, primary in zip(self.names[3:], self.names[:3])}
        def observe():
            return {"nodes": {name: node_rows(self.command(name, ["CLUSTER", "NODES"])) for name in self.names},
                    "replication": {name: fields(self.command(name, ["INFO", "replication"])) for name in self.names[3:]},
                    "health": {name: fields(self.command(name, ["CLUSTER", "INFO"])) for name in self.names}}
        def accepts(views):
            return (all(all(rows[node_id]["parent"] == parent for node_id, parent in parents.items())
                        for rows in views["nodes"].values()) and
                    all(info.get("role") in ("slave", "replica") and info.get("master_link_status") == "up"
                        for info in views["replication"].values()) and
                    all(info.get("cluster_state") == "ok" and info.get("cluster_slots_assigned") == "16384"
                        for info in views["health"].values()))
        self.eventually("real native replicas are following and cluster is healthy", observe, accepts)

    def wait_reply(self, name, command, expected, subset=False, observer=False):
        def matches(reply):
            try:
                assert_equal(reply, expected, subset=subset)
                return True
            except AssertionError:
                return False
        observe = (lambda: self.query(name, command)) if observer else (lambda: self.runtime.execute_node(name, command))
        return self.eventually((name, command, "expected", expected), observe, matches)

    def wait_owner(self, slot, owner):
        slot = int(slot)
        expected = self.ids[owner]
        live = [name for name in self.names if name not in self.paused and
                self.node(name).process is not None and self.node(name).process.poll() is None]
        assert live, "no running cluster nodes to observe"
        self.eventually(("all live nodes agree slot owner", slot, owner),
                        lambda: {name: self.command(name, ["CLUSTER", "SLOTS"]) for name in live},
                        lambda views: all(any(row[0] <= slot <= row[1] and row[2][2] == expected for row in rows)
                                          for rows in views.values()))

    def restart(self, name):
        before = (self.command(name, ["CLUSTER", "MYID"]), self.command(name, ["CLUSTER", "MYSHARDID"]))
        self.node(name).restart_after_crash()
        after = (self.command(name, ["CLUSTER", "MYID"]), self.command(name, ["CLUSTER", "MYSHARDID"]))
        assert before == after, (name, "crash lost durable cluster identity", before, after)
        self.discover()

    def crash(self, name):
        node = self.node(name)
        node.close_clients()
        assert node.process is not None and node.process.poll() is None, (name, "not running")
        node.process.kill()
        node.process.wait(timeout=10)
        self.paused.discard(name)

    def pause(self, name):
        assert name not in self.paused, (name, "already paused")
        process = self.node(name).process
        assert process is not None and process.poll() is None, (name, "not running")
        process.send_signal(signal.SIGSTOP)
        self.paused.add(name)
        deadline = time.monotonic() + 3
        while True:
            pid, status = os.waitpid(process.pid, os.WUNTRACED | os.WNOHANG)
            if pid:
                assert os.WIFSTOPPED(status), (name, "process exited instead of stopping", status)
                break
            assert time.monotonic() < deadline, (name, "SIGSTOP acknowledgement timeout")
            time.sleep(0.005)

    def resume(self, name):
        assert name in self.paused, (name, "not paused")
        self.node(name).process.send_signal(signal.SIGCONT)
        self.paused.remove(name)

    def close(self):
        failures = []
        for proxy in self.proxies.values():
            proxy.release()
        for name in list(self.paused):
            node = self.node(name)
            if node.process is not None and node.process.poll() is None:
                node.process.send_signal(signal.SIGCONT)
            self.paused.discard(name)
        for writer in reversed(list(self.writers.values())):
            try:
                writer.close()
            except BaseException as error:
                failures.append(error)
        for reservations in self.reservations.values():
            for reservation in reservations:
                reservation.close()
        self.reservations.clear()
        for proxy in reversed(list(self.proxies.values())):
            try:
                proxy.close()
            except BaseException as error:
                failures.append(error)
        assert not failures, ("cluster fixture cleanup failures", failures)


class ClusterWriter:
    """One owned native CLI per node; Events expose acknowledged write barriers."""
    def __init__(self, fixture, name, node, key):
        self.fixture, self.name, self.initial_node, self.key = fixture, name, node, key
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.release = threading.Event()
        self.sent = threading.Event()
        self.progress = threading.Condition()
        self.clients = {}
        self.count = 0
        self.value = None
        self.failure = None
        self.thread = threading.Thread(target=self.run, name=f"cluster-writer-{name}", daemon=True)
        fixture.writers[name] = self
        self.thread.start()
        assert self.ready.wait(10), (name, "writer did not acknowledge readiness")
        self.raise_failure()

    def client(self, name):
        if name not in self.clients:
            self.clients[name] = NativeClient(self.fixture.node(name), f"__cluster_writer_{self.name}")
        return self.clients[name]

    def write(self, name, value):
        for _ in range(16):
            client = self.client(name)
            client.send(["SET", self.key, value])
            self.sent.set()
            reply = client.receive()
            if reply == "OK":
                return name
            assert is_error(reply), (self.name, "writer unexpected reply", reply)
            redirect = reply["value"].split()
            assert len(redirect) == 3 and redirect[0] in ("MOVED", "ASK"), (self.name, "writer failed", reply)
            host, port = redirect[2].rsplit(":", 1)
            targets = [target for target in self.fixture.names if self.fixture.node(target).address == (host, int(port))]
            assert len(targets) == 1, (self.name, "redirect escaped owned nodes", reply)
            name = targets[0]
            if redirect[0] == "ASK":
                assert self.client(name).execute(["ASKING"]) == "OK"
        raise AssertionError((self.name, "writer redirect cycle"))

    def run(self):
        try:
            node = self.initial_node
            while not self.stop.is_set():
                value = f"{self.name}:{self.count + 1}"
                node = self.write(node, value)
                with self.progress:
                    self.count += 1
                    self.value = value
                    self.progress.notify_all()
                if not self.ready.is_set():
                    self.ready.set()
                    assert self.release.wait(30), (self.name, "writer start barrier was never released")
        except BaseException as error:
            self.failure = error
            self.ready.set()
            with self.progress:
                self.progress.notify_all()

    def raise_failure(self):
        assert self.failure is None, (self.name, "writer thread failed", self.failure)

    def advance(self, count=1):
        with self.progress:
            target = self.count + count
            self.release.set()
            deadline = time.monotonic() + 20
            while self.count < target:
                self.raise_failure()
                remaining = deadline - time.monotonic()
                assert remaining > 0, (self.name, "writer acknowledgement barrier timeout", self.count, target)
                self.progress.wait(remaining)
        self.raise_failure()

    def close(self):
        self.stop.set()
        self.release.set()
        self.thread.join(timeout=12)
        if self.thread.is_alive():
            for client in self.clients.values():
                client.close()
            self.thread.join(timeout=2)
        else:
            for client in self.clients.values():
                client.close()
        assert not self.thread.is_alive(), (self.name, "writer thread failed to stop")
        self.raise_failure()
