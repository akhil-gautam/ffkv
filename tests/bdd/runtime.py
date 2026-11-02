"""Owned ffkv processes and persistent native CLI sessions for behavioral tests."""
from __future__ import annotations

import base64
import json
from evidence import Evidence, WireEvidence
import math
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests"))
from queue_smoke import startup_address
from fixtures.module_fixture import build_module


def clean_env():
    env = os.environ.copy()
    for name in ("FFKV_URL", "FFKVCLI_AUTH"):
        env.pop(name, None)
    return env


def quote_argument(value: bytes) -> bytes:
    result = bytearray(b'"')
    for byte in value:
        if 32 <= byte < 127 and byte not in (34, 92):
            result.append(byte)
        elif byte in (34, 92):
            result.extend((92, byte))
        else:
            result.extend(f"\\x{byte:02x}".encode())
    result.extend(b'"')
    return bytes(result)


def is_error(value):
    return isinstance(value, dict) and value.get("type") == "error"


def assert_equal(actual, expected, path="$", subset=False):
    """Compare observable replies without confusing booleans and RESP integers."""
    if isinstance(expected, dict) and set(expected) == {"$approx"}:
        spec = expected["$approx"]
        target = spec["value"] if isinstance(spec, dict) else spec
        tolerance = spec.get("abs", 1e-9) if isinstance(spec, dict) else 1e-9
        value = float(actual)
        assert math.isclose(value, float(target), rel_tol=0, abs_tol=tolerance), (path, actual, expected)
        return
    if isinstance(expected, dict) and set(expected) == {"$one_of"}:
        for candidate in expected["$one_of"]:
            try:
                assert_equal(actual, candidate, path)
                return
            except AssertionError:
                pass
        raise AssertionError((path, actual, expected))
    if subset and isinstance(actual, dict) and actual.get("type") == "set":
        actual = actual["values"]
    if isinstance(expected, dict):
        assert isinstance(actual, dict), (path, actual, expected)
        if not subset:
            assert set(actual) == set(expected), (path, actual, expected)
        for key, value in expected.items():
            assert key in actual, (path, key, actual)
            assert_equal(actual[key], value, f"{path}.{key}", subset)
    elif isinstance(expected, list):
        assert isinstance(actual, list), (path, actual, expected)
        if subset:
            remaining = list(actual)
            for item in expected:
                for index, candidate in enumerate(remaining):
                    try:
                        assert_equal(candidate, item, path, True)
                        remaining.pop(index)
                        break
                    except AssertionError:
                        pass
                else:
                    raise AssertionError((path, "missing member", item, actual))
        else:
            assert len(actual) == len(expected), (path, actual, expected)
            for index, (value, wanted) in enumerate(zip(actual, expected)):
                assert_equal(value, wanted, f"{path}[{index}]")
    elif type(expected) in (int, float):
        assert type(actual) in (int, float) and actual == expected, (path, actual, expected)
    else:
        assert type(actual) is type(expected) and actual == expected, (path, actual, expected)


class NativeClient:
    def __init__(self, runtime, name, version=2):
        self.runtime, self.name = runtime, name
        self.version = version
        self.process = subprocess.Popen(
            [str(runtime.client_binary), "-h", runtime.address[0], "-p", str(runtime.address[1]),
             f"-{version}", "--json"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=0, env=clean_env(),
        )
        self.last = None
        self.received = False
        self.evidence = WireEvidence(runtime.evidence)

    def send(self, args):
        assert self.process.poll() is None, (self.name, "client exited", self.process.returncode)
        arguments = [self.runtime.argument(arg) for arg in args]
        frame = b" ".join(quote_argument(arg) for arg in arguments) + b"\n"
        self.process.stdin.write(frame)
        self.process.stdin.flush()
        self.evidence.sent(arguments)

    def receive(self, timeout=10):
        deadline = time.monotonic() + timeout
        data = bytearray()
        while not data.endswith(b"\n"):
            remaining = deadline - time.monotonic()
            assert remaining > 0, (self.name, "native reply timeout", bytes(data[:1024]))
            ready, _, _ = select.select([self.process.stdout], [], [], remaining)
            assert ready, (self.name, "native reply timeout")
            byte = self.process.stdout.read(1)
            if not byte:
                diagnostics = self.process.stderr.read(65536)
                raise AssertionError((self.name, "native reply EOF", self.process.poll(), diagnostics))
            data.extend(byte)
            assert len(data) <= 64 * 1024 * 1024, "BDD reply exceeded client wire bound"
        self.last = json.loads(data)
        self.received = True
        self.evidence.received(self.last)
        return self.last

    def execute(self, args):
        self.send(args)
        return self.receive()

    def quiet(self, seconds):
        assert not select.select([self.process.stdout], [], [], seconds)[0], (self.name, "unexpected reply")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream is not None:
                stream.close()


class Runtime:
    def __init__(self, userdata, evidence=None):
        self.userdata = dict(userdata)
        self.evidence = evidence if evidence is not None else Evidence()
        self.server_binary = Path(userdata.get("server", ROOT / "target/debug/ffkv")).resolve()
        self.client_binary = Path(userdata.get("client", ROOT / "target/debug/ffkv-cli")).resolve()
        self.temporary = tempfile.TemporaryDirectory(prefix="ffkv-bdd-")
        self.directory = Path(self.temporary.name)
        self.data_directory = self.directory / "data"
        self.extra_args = json.loads(userdata.get("server_args", "[]"))
        self.storage = userdata.get("storage", "memory")
        self.process = None
        self.address = None
        self.clients = {}
        self.versions = {}
        self.saved = {}
        self.last_client = "default"
        self.blocked_clients = set()
        self.nodes = {}
        self.default_node = None
        self.placeholders = {}
        self.cleanups = []
        self.last_native = None
        self._module_fixture = None

    def add_node(self, name, extra_args=(), start=True):
        assert name not in self.nodes, ("duplicate node", name)
        node = Runtime(self.userdata, self.evidence)
        node.extra_args.extend(extra_args)
        node.saved = self.saved
        node.placeholders = self.placeholders
        self.nodes[name] = node
        if start:
            node.start()
            self.placeholders.update({name + "_host": node.address[0], name + "_port": str(node.address[1])})
        return node

    def node(self, name):
        assert name in self.nodes, ("unknown node", name)
        return self.nodes[name]

    def execute_node(self, node, args, client="default"):
        runtime = self.node(node)
        reply = runtime.execute(args, client)
        self.last_native = runtime.client(client)
        return reply

    def start(self):
        assert self.process is None, "BDD server already started"
        assert self.server_binary.is_file(), f"Build the ffkv server: {self.server_binary}"
        assert self.client_binary.is_file(), f"Build the native client: {self.client_binary}"
        self.process = subprocess.Popen(
            [str(self.server_binary), "--bind", "127.0.0.1:0", "--data-dir", str(self.data_directory),
             "--durability", "strict", "--shards", "4", "--storage", self.storage,
             *(str(arg).replace("{{directory}}", str(self.directory)) for arg in self.extra_args)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=clean_env(),
        )
        try:
            self.address = startup_address(self.process)
        except BaseException:
            self.close()
            raise

    def client(self, name="default"):
        if name in self.nodes:
            return self.nodes[name].client()
        if self.nodes and name != "default":
            # Named clients belong to the node on which they were opened.
            # Resolve that identity before choosing a default node for new clients.
            matches = ([self.clients[name]] if name in self.clients else []) + [
                node.clients[name] for node in self.nodes.values() if name in node.clients
            ]
            assert len(matches) <= 1, (
                name, "ambiguous native client name; use an explicit node",
                [client.runtime.address for client in matches],
            )
            if matches:
                return matches[0]
        if self.default_node is not None:
            return self.nodes[self.default_node].client(name)
        if self.process is None:
            self.start()
        if name not in self.clients:
            self.clients[name] = NativeClient(self, name, self.versions.get(name, 2))
        return self.clients[name]

    def set_version(self, name, version):
        if name in self.nodes:
            self.nodes[name].set_version("default", version)
            return
        if self.default_node is not None:
            self.nodes[self.default_node].set_version(name, version)
            return
        assert name not in self.clients, "Choose RESP before using that client"
        assert version in (2, 3)
        self.versions[name] = version

    def module_fixture(self):
        """Compile the native C fixture lazily, once per owned server directory."""
        if self._module_fixture is None:
            self._module_fixture = build_module(self.directory)
        return self._module_fixture

    def argument(self, value):
        if isinstance(value, dict):
            if set(value) == {"base64"}:
                return base64.b64decode(value["base64"], validate=True)
            if set(value) == {"saved"}:
                saved = self.saved[value["saved"]]
                if isinstance(saved, dict) and saved.get("type") == "bytes":
                    return base64.b64decode(saved["base64"], validate=True)
                return self.argument(saved)
            if set(value) == {"repeat"}:
                text, count = value["repeat"]["text"], value["repeat"]["count"]
                assert isinstance(count, int) and 0 <= count <= 8 * 1024 * 1024
                encoded = text.encode()
                assert len(encoded) * count <= 8 * 1024 * 1024
                return encoded * count
            raise AssertionError(("invalid BDD argument", value))
        if isinstance(value, str):
            values = {"host": self.address[0], "port": str(self.address[1]), "directory": str(self.directory)}
            values.update(self.placeholders)
            if "{{module_fixture}}" in value:
                values["module_fixture"] = str(self.module_fixture())
            for name, replacement in values.items():
                value = value.replace("{{" + name + "}}", str(replacement))
            return value.encode()
        if type(value) in (int, float):
            return str(value).encode()
        raise AssertionError(("invalid BDD scalar argument", value))

    def execute(self, args, name="default"):
        assert isinstance(args, list) and args, args
        self.last_client = name
        self.last_native = self.client(name)
        return self.last_native.execute(args)

    def last(self):
        client = self.last_native or self.client(self.last_client)
        assert client.received, "No reply has been received"
        return client.last

    def send(self, args, name="default"):
        self.last_client = name
        self.last_native = self.client(name)
        self.last_native.send(args)

    def blocked_count(self):
        reply = self.client("__observer").execute(["INFO", "clients"])
        assert isinstance(reply, str), reply
        fields = dict(line.split(":", 1) for line in reply.splitlines() if ":" in line)
        return int(fields["blocked_clients"])

    def wait_blocked(self, name, blocked=True):
        target = len(self.blocked_clients) + (name not in self.blocked_clients)
        deadline = time.monotonic() + 5
        while True:
            count = self.blocked_count()
            if (count >= target) if blocked else (count < len(self.blocked_clients)):
                break
            assert time.monotonic() < deadline, (name, "blocked-state timeout", count, target)
            time.sleep(0.01)
        if blocked:
            self.client(name).quiet(0)
            self.blocked_clients.add(name)
        else:
            self.blocked_clients.discard(name)

    def receive(self, name):
        self.last_client = name
        self.last_native = self.client(name)
        result = self.last_native.receive()
        self.blocked_clients.discard(name)
        return result

    def disconnect(self, name):
        self.clients.pop(name).close()
        if name in self.blocked_clients:
            self.wait_blocked(name, False)

    def restart_after_crash(self):
        self.close_clients()
        assert self.process is not None
        self.process.kill()
        self.process.wait(timeout=10)
        self.process.stderr.close()
        self.process = None
        self.address = None
        self.start()

    def close_clients(self):
        for client in self.clients.values():
            client.close()
        self.clients.clear()
        self.blocked_clients.clear()

    def close(self):
        failures = []
        while self.cleanups:
            try:
                self.cleanups.pop()()
            except Exception as error:
                failures.append(error)
        for node in self.nodes.values():
            try:
                node.close()
            except Exception as error:
                failures.append(error)
        self.nodes.clear()
        self.close_clients()
        if self.process is not None:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
            self.process.stderr.close()
            self.process = None
        self.temporary.cleanup()
        if failures:
            raise failures[0]
