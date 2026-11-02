#!/usr/bin/env python3
"""Real stdio MCP behavioral smoke; owns every server, child and data directory.

Run after building ffkv and ffkv-mcp. No service discovery or external targets.
The coverage map names assertions actually performed, not all catalog options.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time

from cli_smoke import Client
from durability_smoke import Server

ROOT = Path(__file__).resolve().parents[1]
TOOLS = {"ffkv_execute", "ffkv_session", "ffkv_commands", "ffkv_result",
         "ffkv_management", "ffkv_management_live", "ffkv_process"}
FEATURE_COVERAGE = {
    "protocol": ["legacy initialize", "modern server/discover with request metadata", "tool schemas", "capability and command resources", "live command pagination"],
    "datatypes": ["binary strings", "64-bit integers", "ordered pipeline", "RESP3 map/set/boolean/double tags", "hash/list/set/sorted-set", "stream", "bitmap", "HyperLogLog", "geo", "JSONPath", "sparse arrays", "timeseries", "vector FP32", "Bloom/Cuckoo", "count-min sketch", "top-K", "t-digest", "search numeric query"],
    "jobs": ["FQ.ENQUEUE idempotency", "FQ.CLAIM receipt", "FQ.EXTEND", "FQ.ACK", "FQ.GET and STATS"],
    "sessions": ["explicit open/list/close", "SELECT isolation", "MULTI/EXEC", "WATCH abort", "blocking wakeup"],
    "streams": ["RESP3 subscription acknowledgement and push", "raw fragmented RESP bytes", "bounded receive retaining unread bytes", "manual-session execute rejection"],
    "errors": ["typed server error", "whole-batch validation before writes", "timeout closes connection with unknown outcome", "cancellation no replay", "ACL denial", "invalid session and target", "queued send timeout preserves active execute"],
    "administration": ["EVAL state mutation", "CONFIG GET", "INFO", "CLIENT ID", "SCAN MATCH COUNT", "expiry"],
    "management": ["authenticated HTTP overview and metrics", "query parameters", "confirmed POST", "authenticated live WebSocket", "snapshot file bytes and SHA256", "path traversal and overwrite rejection", "active receive survives idle TTL", "pre-dispatch POST has known non-execution", "absolute configured root aliases preserve confinement"],
    "process": ["real server and restore help", "owned start/status/logs/stop", "unknown-handle refusal", "offline sealed backup restore and restored GET", "existing destination refusal", "process-disabled refusal"],
    "artifacts": ["large binary response paging", "bounded page lengths and offsets", "byte-exact reconstruction", "explicit release", "bidirectional pipeline beyond socket buffers"],
    "benchmark": ["large bidirectional benchmark pipelines return exact values on both transports"],
}


def binary(name, supplied=None):
    if supplied:
        path = Path(supplied).resolve()
    else:
        candidates = [ROOT / "target/release" / name, ROOT / "target/debug" / name]
        path = next((p for p in candidates if p.is_file()), candidates[-1])
    if not path.is_file():
        raise AssertionError(f"Build required binary first: {path}")
    return path


def b64(value):
    return {"base64": base64.b64encode(value).decode("ascii")}


def argument(value):
    return b64(value) if isinstance(value, bytes) else value


def resp(frame):
    """Decode tagged results for assertions, preserving errors and byte values."""
    kind = frame["type"]
    if kind in ("simple", "bulk", "error", "big_number", "verbatim"):
        value = (base64.b64decode(frame["value"], validate=True)
                 if frame["encoding"] == "base64" else frame["value"].encode())
        return ("error", value) if kind == "error" else value
    if kind == "integer":
        assert isinstance(frame["value"], str), frame
        return int(frame["value"])
    if kind == "double":
        return float(frame["value"])
    if kind == "boolean":
        return frame["value"]
    if kind == "null":
        return None
    if kind in ("array", "set", "push"):
        return [resp(item) for item in frame["items"]]
    if kind == "map":
        return [(resp(key), resp(value)) for key, value in frame["entries"]]
    if kind == "attribute":
        return resp(frame["value"])
    raise AssertionError(("unknown RESP tag", frame))


def mapping(value):
    if isinstance(value, dict):
        return value
    if value and isinstance(value[0], tuple):
        return dict(value)
    assert isinstance(value, list) and len(value) % 2 == 0, value
    return dict(zip(value[::2], value[1::2]))


def eventually(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        assert time.monotonic() < deadline, "owned fixture condition timed out"
        time.sleep(0.02)


class Mcp:
    """Multiplex JSON-RPC over one real persistent stdio process."""
    def __init__(self, executable, config, modern=False):
        self.modern = modern
        self.condition = threading.Condition()
        self.responses, self.notifications = {}, []
        self.failure = None
        self.counter = 0
        self.write_lock = threading.Lock()
        self.stderr = tempfile.TemporaryFile()
        env = os.environ.copy()
        for key in ("FFKV_URL", "FFKVCLI_AUTH", "FFKV_MCP_AUTH"):
            env.pop(key, None)
        self.process = subprocess.Popen([str(executable), "--config", str(config)],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr, env=env)
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            if modern:
                self.negotiation = self.request("server/discover", {})
                assert "2026-07-28" in self.negotiation["supportedVersions"], self.negotiation
            else:
                self.negotiation = self.request("initialize", {
                    "protocolVersion": "2025-11-25", "capabilities": {},
                    "clientInfo": {"name": "ffkv-mcp-smoke", "version": "1"}})
                assert self.negotiation["protocolVersion"] == "2025-11-25", self.negotiation
                self.notify("notifications/initialized", {})
        except BaseException:
            self.close()
            raise

    def _read(self):
        try:
            while True:
                line = self.process.stdout.readline(128 * 1024 * 1024)
                if not line:
                    raise EOFError("MCP stdout closed")
                assert line.endswith(b"\n"), "MCP response exceeded harness bound"
                message = json.loads(line)
                assert message.get("jsonrpc") == "2.0", message
                with self.condition:
                    if "id" in message:
                        self.responses[message["id"]] = message
                    else:
                        self.notifications.append(message)
                    self.condition.notify_all()
        except BaseException as error:
            with self.condition:
                self.failure = error
                self.condition.notify_all()

    def _send(self, message):
        with self.write_lock:
            self.process.stdin.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
            self.process.stdin.flush()

    def begin(self, method, params=None):
        with self.condition:
            self.counter += 1
            request_id = self.counter
        params = dict(params or {})
        if self.modern:
            params["_meta"] = {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {"name": "ffkv-mcp-smoke", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {},
            }
        self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        return request_id

    def finish(self, request_id, timeout=20):
        deadline = time.monotonic() + timeout
        with self.condition:
            while request_id not in self.responses:
                if self.failure:
                    self.stderr.seek(0)
                    raise AssertionError((self.failure, self.stderr.read(65536)))
                remaining = deadline - time.monotonic()
                assert remaining > 0, ("MCP response timeout", request_id)
                self.condition.wait(remaining)
            message = self.responses.pop(request_id)
        assert "error" not in message, message
        return message["result"]

    def request(self, method, params=None, timeout=20):
        return self.finish(self.begin(method, params), timeout)

    def notify(self, method, params):
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    @staticmethod
    def payload(result):
        if "structuredContent" in result:
            return result["structuredContent"]
        texts = [item["text"] for item in result["content"] if item["type"] == "text"]
        assert len(texts) == 1, result
        return json.loads(texts[0])

    def tool(self, name, args, error=False, resolve=True):
        result = self.request("tools/call", {"name": name, "arguments": args}, timeout=45)
        assert bool(result.get("isError", False)) == error, (name, args, result)
        value = self.payload(result)
        if resolve and isinstance(value, dict) and value.get("read_tool") == "ffkv_result":
            value = self.read_artifact(value)
        return value

    def read_artifact(self, handle, page_size=65536):
        data, offset = bytearray(), 0
        while True:
            page = self.tool("ffkv_result", {"action": "read", "result_id": handle["result_id"],
                                              "offset": offset, "length": page_size}, resolve=False)
            chunk = base64.b64decode(page["data_base64"], validate=True)
            assert page["offset"] == offset and 0 < len(chunk) <= page_size, page
            assert page["total_bytes"] == handle["total_bytes"], page
            data.extend(chunk)
            offset += len(chunk)
            if page["next_offset"] is None:
                break
            assert page["next_offset"] == offset, page
        assert len(data) == handle["total_bytes"], handle
        self.tool("ffkv_result", {"action": "release", "result_id": handle["result_id"]}, resolve=False)
        return json.loads(data)

    def execute(self, commands, session=None, timeout_ms=None, outcome="ok"):
        args = {"commands": [[argument(item) for item in command] for command in commands]}
        if session is not None:
            args["session"] = session
        if timeout_ms is not None:
            args["timeout_ms"] = timeout_ms
        result = self.tool("ffkv_execute", args, error=outcome != "ok")
        assert result["outcome"] == outcome, result
        assert len(result["replies"]) == len(commands), result
        return [resp(frame) for frame in result["replies"]]

    def command(self, *args, **kwargs):
        return self.execute([args], **kwargs)[0]

    def open(self, **kwargs):
        return self.tool("ffkv_session", {"action": "open", **kwargs})["session"]

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        self.reader.join(timeout=2)
        for stream in (self.process.stdin, self.process.stdout, self.stderr):
            stream.close()


class Fixture:
    def __init__(self, server_binary=None, mcp_binary=None):
        self.server_binary = binary("ffkv", server_binary)
        self.mcp_binary = binary("ffkv-mcp", mcp_binary)
        self.temporary = tempfile.TemporaryDirectory(prefix="ffkv-mcp-")
        self.directory = Path(self.temporary.name)
        self.mcps, self.servers = [], []
        self.completed = []
        try:
            token = "ffkv-owned-mcp-smoke-token-" + os.urandom(16).hex()
            policy = self.directory / "management-policy.json"
            policy.write_text(json.dumps({"tokens": [{"name": "mcp-smoke", "role": "admin",
                                                       "sha256": hashlib.sha256(token.encode()).hexdigest()}]}))
            secret = self.directory / "management-token"
            secret.write_text(token)
            policy.chmod(0o600)
            secret.chmod(0o600)
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                self.management_port = reservation.getsockname()[1]
            self.server = Server(str(self.server_binary), self.directory / "data", "strict", extra_args=[
                "--management-bind", f"127.0.0.1:{self.management_port}",
                "--management-token-file", str(policy), "--replication-export"])
            self.servers.append(self.server)
            self.server.start()
            self.files = self.directory / "files"
            self.files.mkdir()
            self.config = {
                "targets": {"default": {"host": self.server.address[0], "port": self.server.address[1],
                                        "protocol": 2, "management_url": f"http://127.0.0.1:{self.management_port}",
                                        "management_token_file": str(secret)}},
                "default_target": "default", "file_root": str(self.files),
                "process": {"enabled": True, "binary": str(self.server_binary)},
                "limits": {"inline_result_bytes": 4096},
            }
            self.config_path = self.directory / "mcp.json"
            self.config_path.write_text(json.dumps(self.config))
            self.mcp = self.new_mcp()
        except BaseException:
            self.close()
            raise

    def new_mcp(self, modern=False, config=None):
        value = Mcp(self.mcp_binary, config or self.config_path, modern)
        self.mcps.append(value)
        return value

    def run(self, group):
        assert group in FEATURE_COVERAGE, group
        getattr(self, "check_" + group)()
        self.completed.append(group)

    def check_protocol(self):
        for client in (self.mcp, self.new_mcp(modern=True)):
            tools = client.request("tools/list")["tools"]
            assert {tool["name"] for tool in tools} == TOOLS, tools
            for tool in tools:
                schema = tool["inputSchema"]
                assert schema["type"] == "object" and schema["additionalProperties"] is False, tool
                assert isinstance(schema["properties"], dict) and schema["properties"], tool
            assert client.command("PING") == b"PONG"
            resources = client.request("resources/list")["resources"]
            assert {"ffkv://capabilities", "ffkv://commands"} <= {r["uri"] for r in resources}, resources
            for uri in ("ffkv://capabilities", "ffkv://commands"):
                contents = client.request("resources/read", {"uri": uri})["contents"]
                assert contents and all(item["uri"] == uri for item in contents), contents
                for item in contents:
                    assert json.loads(item["text"]) is not None
            names, cursor = [], None
            while True:
                page = client.tool("ffkv_commands", {"limit": 17, **({"cursor": cursor} if cursor else {})})
                assert 0 < len(page["commands"]) <= 17, page
                names.extend(row["name"].lower() for row in page["commands"])
                cursor = page["next_cursor"]
                if cursor is None:
                    break
                assert int(cursor) == len(names), page
            assert len(names) == len(set(names)) == page["total"], names
            assert {"get", "set", "fq.enqueue"} <= set(names), names
            filtered = client.tool("ffkv_commands", {"prefix": "fq.", "limit": 200, "refresh": True})
            assert filtered["commands"] and all(row["name"].lower().startswith("fq.") for row in filtered["commands"])

    def check_datatypes(self):
        c = self.mcp
        key, value = b"mcp:binary\x00\xff", b"\x00\xff\r\n$3\r\nabc"
        assert c.execute([["SET", key, value], ["GET", key], ["INCRBY", "mcp:integer", "9007199254740993"]]) == [b"OK", value, 9007199254740993]
        cases = [
            ([['HSET', 'mcp:hash', 'field', 'value'], ['HGET', 'mcp:hash', 'field']], [1, b'value']),
            ([['RPUSH', 'mcp:list', 'a', 'b'], ['LRANGE', 'mcp:list', 0, -1]], [2, [b'a', b'b']]),
            ([['SADD', 'mcp:set', 'a'], ['SISMEMBER', 'mcp:set', 'a']], [1, 1]),
            ([['ZADD', 'mcp:zset', '1.5', 'a'], ['ZRANGE', 'mcp:zset', 0, -1]], [1, [b'a']]),
            ([['XADD', 'mcp:stream', '1-0', 'f', 'v'], ['XLEN', 'mcp:stream']], [b'1-0', 1]),
            ([['SETBIT', 'mcp:bits', 9, 1], ['BITCOUNT', 'mcp:bits']], [0, 1]),
            ([['PFADD', 'mcp:hll', 'a'], ['PFCOUNT', 'mcp:hll']], [1, 1]),
            ([['GEOADD', 'mcp:geo', '13.361389', '38.115556', 'Palermo'], ['ZCARD', 'mcp:geo']], [1, 1]),
            ([['JSON.SET', 'mcp:json', '$', '{"a":7}'], ['JSON.GET', 'mcp:json', '$.a']], [b'OK', b'[7]']),
            ([['ARSET', 'mcp:array', 2, 'two'], ['ARMGET', 'mcp:array', 0, 2]], [1, [None, b'two']]),
            ([['TS.CREATE', 'mcp:ts'], ['TS.ADD', 'mcp:ts', 1000, 2]], [b'OK', 1000]),
            ([['VADD', 'mcp:vector', 'FP32', b64(bytes.fromhex('0000803f0000000000000000')), 'east', 'NOQUANT'], ['VDIM', 'mcp:vector']], [1, 3]),
            ([['BF.ADD', 'mcp:bf', 'item'], ['BF.EXISTS', 'mcp:bf', 'item']], [1, 1]),
            ([['CF.ADD', 'mcp:cf', 'item'], ['CF.EXISTS', 'mcp:cf', 'item']], [1, 1]),
            ([['CMS.INITBYDIM', 'mcp:cms', 128, 4], ['CMS.INCRBY', 'mcp:cms', 'item', 3], ['CMS.QUERY', 'mcp:cms', 'item']], [b'OK', [3], [3]]),
            ([['TOPK.RESERVE', 'mcp:topk', 2, 64, 5, '0.9'], ['TOPK.INCRBY', 'mcp:topk', 'item', 20], ['TOPK.LIST', 'mcp:topk', 'WITHCOUNT']], [b'OK', [None], [b'item', 20]]),
            ([['TDIGEST.CREATE', 'mcp:tdigest'], ['TDIGEST.ADD', 'mcp:tdigest', 10, 20]], [b'OK', b'OK']),
            ([['HSET', 'mcp:doc:1', 'n', '7'], ['FT.CREATE', 'mcp:idx', 'ON', 'HASH', 'PREFIX', 1, 'mcp:doc:', 'SCHEMA', 'n', 'NUMERIC'], ['FT.SEARCH', 'mcp:idx', '@n:[7 7]', 'NOCONTENT', 'DIALECT', 2]], [1, b'OK', [1, b'mcp:doc:1']]),
        ]
        for commands, expected in cases:
            assert c.execute(commands) == expected, commands
        assert c.command('XRANGE', 'mcp:stream', '-', '+') == [[b'1-0', [b'f', b'v']]]
        assert float(c.command('TS.GET', 'mcp:ts')[1]) == 2
        assert [float(value) for value in c.command('TDIGEST.QUANTILE', 'mcp:tdigest', 0, 1)] == [10, 20]
        session = c.open(protocol=3)
        typed = c.tool('ffkv_execute', {'session': session, 'commands': [
            ['HGETALL', 'mcp:hash'], ['SMEMBERS', 'mcp:set'],
            ['BF.EXISTS', 'mcp:bf', 'item'], ['TS.GET', 'mcp:ts']]})
        assert typed['outcome'] == 'ok' and len(typed['replies']) == 4, typed
        frames = typed['replies']
        assert frames[0]['type'] == 'map' and mapping(resp(frames[0])) == {b'field': b'value'}, frames[0]
        assert frames[1]['type'] == 'set' and resp(frames[1]) == [b'a'], frames[1]
        assert frames[2] == {'type': 'boolean', 'value': True}, frames[2]
        assert frames[3]['items'][1]['type'] == 'double' and resp(frames[3]) == [1000, 2.0], frames[3]
        c.tool('ffkv_session', {'action': 'close', 'session': session})

    def check_jobs(self):
        c = self.mcp
        job = mapping(c.command('FQ.ENQUEUE', 'mcp:jobs', b'payload\x00\xff', 'IDEMPOTENCY', 'same'))
        again = mapping(c.command('FQ.ENQUEUE', 'mcp:jobs', 'ignored', 'IDEMPOTENCY', 'same'))
        assert job[b'created'] == 1 and again[b'created'] == 0 and job[b'job_id'] == again[b'job_id']
        rows = c.command('FQ.CLAIM', 'mcp:jobs', 'COUNT', 1, 'VISIBILITY', 30000)
        assert len(rows) == 1, rows
        claimed = mapping(rows[0])
        assert claimed[b'payload'] == b'payload\x00\xff' and claimed[b'job_id'] == job[b'job_id'], claimed
        deadline = c.command('FQ.EXTEND', 'mcp:jobs', claimed[b'receipt'], 60000)
        assert deadline > claimed[b'deadline_ms'], deadline
        assert mapping(c.command('FQ.GET', 'mcp:jobs', job[b'job_id']))[b'deadline_ms'] == deadline
        assert c.command('FQ.ACK', 'mcp:jobs', claimed[b'receipt']) == b'OK'
        assert c.command('FQ.GET', 'mcp:jobs', job[b'job_id']) is None
        assert mapping(c.command('FQ.STATS', 'mcp:jobs'))[b'jobs'] == 0

    def check_sessions(self):
        c = self.mcp
        one, two = c.open(), c.open()
        assert c.execute([['SELECT', 1], ['SET', 'mcp:db', 'one']], session=one) == [b'OK', b'OK']
        assert c.command('GET', 'mcp:db', session=two) is None
        assert c.execute([['MULTI'], ['INCR', 'mcp:tx'], ['INCR', 'mcp:tx'], ['EXEC']], session=two) == [b'OK', b'QUEUED', b'QUEUED', [1, 2]]
        assert c.command('WATCH', 'mcp:watch', session=two) == b'OK'
        assert c.command('SET', 'mcp:watch', 'other') == b'OK'
        assert c.execute([['MULTI'], ['SET', 'mcp:watch', 'lost'], ['EXEC']], session=two) == [b'OK', b'QUEUED', None]
        assert c.command('GET', 'mcp:watch') == b'other'
        request = c.begin('tools/call', {'name': 'ffkv_execute', 'arguments': {'session': two, 'commands': [['BLPOP', 'mcp:block', '3']]}})
        eventually(lambda: b'blocked_clients:1' in c.command('INFO', 'clients'))
        assert c.command('RPUSH', 'mcp:block', 'ready') == 1
        result = c.finish(request)
        assert not result.get('isError', False), result
        assert resp(c.payload(result)['replies'][0]) == [b'mcp:block', b'ready']
        listed = c.tool('ffkv_session', {'action': 'list'})
        assert one in json.dumps(listed) and two in json.dumps(listed), listed
        for session in (one, two):
            c.tool('ffkv_session', {'action': 'close', 'session': session})
            c.tool('ffkv_execute', {'session': session, 'commands': [['PING']]}, error=True)

    def check_streams(self):
        c = self.mcp
        session = c.open(protocol=3)
        ack = c.tool('ffkv_session', {'action': 'send', 'session': session, 'commands': [['SUBSCRIBE', 'mcp:channel']]})
        assert ack['outcome'] == 'written', ack
        received = c.tool('ffkv_session', {'action': 'receive', 'session': session, 'max_frames': 1, 'wait_ms': 2000})
        assert [resp(f) for f in received['frames']] == [[b'subscribe', b'mcp:channel', 1]], received
        assert c.command('PUBLISH', 'mcp:channel', b'push\x00\xff') == 1
        received = c.tool('ffkv_session', {'action': 'receive', 'session': session, 'max_frames': 1, 'wait_ms': 2000})
        assert received['frames'][0]['type'] == 'push', received
        assert resp(received['frames'][0]) == [b'message', b'mcp:channel', b'push\x00\xff']
        c.tool('ffkv_execute', {'session': session, 'commands': [['PING']]}, error=True)
        c.tool('ffkv_session', {'action': 'close', 'session': session})
        raw = c.open(mode='raw')
        frame = Client.frame(['PING'])
        c.tool('ffkv_session', {'action': 'send', 'session': raw, 'data_base64': b64(frame[:5])['base64']})
        empty = c.tool('ffkv_session', {'action': 'receive', 'session': raw, 'max_bytes': 3, 'wait_ms': 20})
        assert empty['timed_out'] and empty['bytes'] == 0, empty
        c.tool('ffkv_session', {'action': 'send', 'session': raw, 'data_base64': b64(frame[5:] + frame)['base64']})
        data = bytearray()
        while len(data) < len(b'+PONG\r\n' * 2):
            part = c.tool('ffkv_session', {'action': 'receive', 'session': raw, 'max_bytes': 3, 'wait_ms': 2000})
            chunk = base64.b64decode(part['data_base64'], validate=True)
            assert 0 < len(chunk) <= 3 and part['bytes'] == len(chunk), part
            data.extend(chunk)
        assert data == b'+PONG\r\n' * 2, data
        c.tool('ffkv_session', {'action': 'close', 'session': raw})

    def check_errors(self):
        c = self.mcp
        assert c.command('SET', 'mcp:wrongtype', 'text') == b'OK'
        result = c.execute([['LPUSH', 'mcp:wrongtype', 'x'], ['PING']], outcome='error')
        assert result[0][0] == 'error' and result[1] == b'PONG', result
        c.tool('ffkv_execute', {'commands': [['SET', 'mcp:invalid-batch', 'bad'], ['SET', 'x', {'base64': '!invalid!'}]]}, error=True)
        assert c.command('EXISTS', 'mcp:invalid-batch') == 0
        c.tool('ffkv_session', {'action': 'open', 'target': 'unconfigured'}, error=True)
        c.tool('ffkv_execute', {'session': 'unowned', 'commands': [['PING']]}, error=True)
        c.tool('ffkv_execute', {'commands': [['SET', 'mcp:forbidden-batch', 'bad'], ['SUBSCRIBE', 'channel']]}, error=True)
        assert c.command('EXISTS', 'mcp:forbidden-batch') == 0
        session = c.open()
        failed = c.tool('ffkv_execute', {'session': session, 'commands': [['INCR', 'mcp:once'], ['BLPOP', 'mcp:timeout', 0]], 'timeout_ms': 100}, error=True)
        assert 'unknown' in json.dumps(failed).lower(), failed
        assert c.command('GET', 'mcp:once') == b'1'
        c.tool('ffkv_execute', {'session': session, 'commands': [['PING']]}, error=True)
        session = c.open()
        request = c.begin('tools/call', {'name': 'ffkv_execute', 'arguments': {'session': session, 'commands': [['INCR', 'mcp:cancel-once'], ['BLPOP', 'mcp:cancel', 0]], 'timeout_ms': 10000}})
        eventually(lambda: c.command('GET', 'mcp:cancel-once') == b'1')
        c.notify('notifications/cancelled', {'requestId': request, 'reason': 'owned smoke cancellation'})
        eventually(lambda: b'blocked_clients:0' in c.command('INFO', 'clients'))
        assert c.command('GET', 'mcp:cancel-once') == b'1'
        c.tool('ffkv_execute', {'session': session, 'commands': [['PING']]}, error=True)
        assert c.command('ACL', 'SETUSER', 'mcp-read', 'on', '>owned-password', '~mcp:*', '+get', '+ping') == b'OK'
        restricted = c.open()
        assert c.command('AUTH', 'mcp-read', 'owned-password', session=restricted) == b'OK'
        denied = c.command('SET', 'mcp:denied', 'x', session=restricted, outcome='error')
        assert denied[0] == 'error' and b'NOPERM' in denied[1], denied
        assert c.command('EXISTS', 'mcp:denied') == 0
        c.tool('ffkv_session', {'action': 'close', 'session': restricted})
        assert c.command('ACL', 'DELUSER', 'mcp-read') == 1
        # Waiting to dispatch must not poison a different command already using
        # the session, nor send the queued command after its deadline expires.
        queued_config = dict(self.config, limits=dict(self.config['limits'], operation_timeout_ms=250))
        config_path = self.directory / 'queued-send.json'
        config_path.write_text(json.dumps(queued_config))
        queued = self.new_mcp(config=config_path)
        session = queued.open(protocol=2)
        assert queued.command('CLIENT', 'SETNAME', 'mcp-queued-send', session=session) == b'OK'
        blocked = queued.begin('tools/call', {'name': 'ffkv_execute', 'arguments': {
            'session': session, 'commands': [['BLPOP', 'mcp:queued-send', 0]], 'timeout_ms': 10000}})
        eventually(lambda: any(b'name=mcp-queued-send' in row and b'flags=b' in row
                               for row in c.command('CLIENT', 'LIST').splitlines()))
        queued.tool('ffkv_session', {'action': 'send', 'session': session,
                                    'commands': [['INCR', 'mcp:queued-not-written']]}, error=True)
        assert c.command('RPUSH', 'mcp:queued-send', 'preserved') == 1
        result = queued.finish(blocked)
        assert not result.get('isError', False), result
        assert [resp(frame) for frame in queued.payload(result)['replies']] == [[b'mcp:queued-send', b'preserved']]
        assert c.command('EXISTS', 'mcp:queued-not-written') == 0
        assert queued.command('PING', session=session) == b'PONG'
        queued.tool('ffkv_session', {'action': 'close', 'session': session})

    def check_administration(self):
        c = self.mcp
        assert c.command('EVAL', "return redis.call('INCR',KEYS[1])", 1, 'mcp:script') == 1
        assert c.command('GET', 'mcp:script') == b'1'
        assert b'redis_version:' in c.command('INFO', 'server')
        assert c.command('CLIENT', 'ID') > 0
        assert mapping(c.command('CONFIG', 'GET', 'maxclients'))[b'maxclients']
        assert c.command('SET', 'mcp:expiry', 'value', 'PX', 30000, 'NX') == b'OK'
        assert 0 < c.command('PTTL', 'mcp:expiry') <= 30000
        cursor, found = b'0', []
        while True:
            cursor, keys = c.command('SCAN', cursor, 'MATCH', 'mcp:exp*', 'COUNT', 2)
            found.extend(keys)
            if cursor == b'0':
                break
        assert found == [b'mcp:expiry'], found
        assert c.command('PERSIST', 'mcp:expiry') == 1
        assert c.command('PTTL', 'mcp:expiry') == -1

    def check_management(self):
        c = self.mcp
        overview = c.tool('ffkv_management', {'path': '/api/overview'})
        assert overview['status'] == 200 and overview['body']['encoding'] == 'json', overview
        assert isinstance(overview['body']['value'], dict) and overview['body']['value'], overview
        metrics = c.tool('ffkv_management', {'path': '/metrics'})
        assert metrics['status'] == 200 and 'ffkv_' in metrics['body']['value'], metrics
        keys = c.tool('ffkv_management', {'path': '/api/keys?count=2'})
        assert keys['status'] == 200, keys
        unconfirmed = c.tool('ffkv_management', {'method': 'POST', 'path': '/api/storage/action', 'body': {'action': 'checkpoint'}})
        assert 400 <= unconfirmed['status'] < 500, unconfirmed
        confirmed = c.tool('ffkv_management', {'method': 'POST', 'path': '/api/storage/action', 'body': {'action': 'checkpoint', 'confirmed': True}})
        assert confirmed['status'] == 200, confirmed
        stream = c.tool('ffkv_management_live', {'action': 'open'})['stream']
        listing = c.tool('ffkv_management_live', {'action': 'list'})
        assert stream in json.dumps(listing), listing
        event = c.tool('ffkv_management_live', {'action': 'receive', 'stream': stream, 'max_frames': 1, 'wait_ms': 3000})
        assert len(event['frames']) == 1 and event['frames'][0]['encoding'] == 'json', event
        assert isinstance(event['frames'][0]['value'], dict) and event['frames'][0]['value'], event
        c.tool('ffkv_management_live', {'action': 'close', 'stream': stream})
        c.tool('ffkv_management_live', {'action': 'receive', 'stream': stream}, error=True)
        download = c.tool('ffkv_management', {'path': '/api/replication/snapshot', 'download_path': 'snapshot.ffkv'})
        assert download['status'] == 200, download
        info = download['download']
        path = Path(info['path']).resolve()
        assert path.is_relative_to(self.files.resolve()) and path.is_file(), info
        data = path.read_bytes()
        assert len(data) == info['bytes'] > 0 and hashlib.sha256(data).hexdigest() == info['sha256'], info
        c.tool('ffkv_management', {'path': '/api/replication/snapshot', 'download_path': 'snapshot.ffkv'}, error=True)
        assert path.read_bytes() == data
        for path in ('//example.invalid/api/overview', '/api/../metrics', '/api/overview#secret'):
            c.tool('ffkv_management', {'path': path}, error=True)
        c.tool('ffkv_management', {'path': '/api/replication/snapshot', 'download_path': '../outside.ffkv'}, error=True)
        assert not (self.directory / 'outside.ffkv').exists()
        rejected = c.tool('ffkv_management', {'method': 'POST', 'path': '/api/../x'}, error=True)
        assert 'unknown' not in rejected['error'].lower(), rejected

        alias = self.directory / 'download-alias'
        alias.symlink_to(self.files, target_is_directory=True)
        alias_config = dict(self.config, file_root=str(alias))
        config_path = self.directory / 'alias.json'
        config_path.write_text(json.dumps(alias_config))
        aliased = self.new_mcp(config=config_path)
        exported = aliased.tool('ffkv_management', {
            'path': '/api/replication/snapshot', 'download_path': str(alias / 'absolute.ffkv')})
        exported_path = Path(exported['download']['path'])
        assert exported['status'] == 200 and exported_path == (self.files / 'absolute.ffkv').resolve(), exported
        assert hashlib.sha256(exported_path.read_bytes()).hexdigest() == exported['download']['sha256']
        # The operator's root alias is allowed; symlinks below that boundary
        # remain forbidden even when their destinations are inside the root.
        nested = self.files / 'nested'
        nested.mkdir()
        (self.files / 'child-link').symlink_to(nested, target_is_directory=True)
        for destination in (str(alias / 'child-link' / 'escape.ffkv'),
                            str(self.directory / 'outside-alias.ffkv')):
            aliased.tool('ffkv_management', {
                'path': '/api/replication/snapshot', 'download_path': destination}, error=True)
        assert not (nested / 'escape.ffkv').exists()
        assert not (self.directory / 'outside-alias.ffkv').exists()

        idle_config = dict(self.config, limits=dict(self.config['limits'], session_idle_ms=1000))
        config_path = self.directory / 'idle-live.json'
        config_path.write_text(json.dumps(idle_config))
        active = self.new_mcp(config=config_path)
        stream = active.tool('ffkv_management_live', {'action': 'open'})['stream']
        events = active.tool('ffkv_management_live', {
            'action': 'receive', 'stream': stream, 'max_frames': 10, 'wait_ms': 2500})
        assert events['timed_out'] is True and events['closed'] is False, events
        assert events['frames'] and all('server' in frame['value'] and 'metrics' in frame['value']
                                       for frame in events['frames']), events
        listed = active.tool('ffkv_management_live', {'action': 'list'})['streams']
        assert any(row['stream'] == stream for row in listed), listed
        eventually(lambda: not active.tool('ffkv_management_live', {'action': 'list'})['streams'])

    def check_process(self):
        c = self.mcp
        for args in ([], ['restore']):
            help_result = c.tool('ffkv_process', {'action': 'help', 'args': args})
            assert help_result['success'] is True and help_result['exit_code'] == 0, help_result
            assert help_result['logs']['entries'], help_result
            text = ''.join(entry.get('text', '') for entry in help_result['logs']['entries'])
            assert '--data-dir' in text, text
        owned_data = self.directory / 'owned-data'
        started = c.tool('ffkv_process', {'action': 'start', 'args': ['--bind', '127.0.0.1:0', '--data-dir', str(owned_data)]})
        handle = started['process']
        assert started['state'] == 'running' and started['pid'] > 0, started
        status = c.tool('ffkv_process', {'action': 'status', 'process': handle})
        assert status['pid'] == started['pid'] and status['state'] == 'running', status
        logs = eventually(lambda: self._owned_logs(handle))
        assert logs['next_sequence'] > 0 and logs['dropped_bytes'] == 0, logs
        next_page = c.tool('ffkv_process', {'action': 'logs', 'process': handle, 'since_sequence': logs['next_sequence'], 'max_bytes': 4096})
        assert next_page['next_sequence'] >= logs['next_sequence'], next_page
        stopped = c.tool('ffkv_process', {'action': 'stop', 'process': handle})
        assert stopped['state'] == 'exited', stopped
        assert c.tool('ffkv_process', {'action': 'status', 'process': handle})['state'] == 'exited'
        c.tool('ffkv_process', {'action': 'stop', 'process': str(self.server.process.pid)}, error=True)
        assert c.command('PING') == b'PONG'
        assert c.command('SET', 'mcp:restore', 'sealed') == b'OK'
        c.command('BACKUP', 'START')
        eventually(lambda: mapping(c.command('BACKUP', 'STATUS')).get(b'state') == b'incrementing', timeout=30)
        c.command('BACKUP', 'SEAL')
        files = [Path(value.decode()).resolve() for value in c.command('BACKUP', 'LIST')]
        assert files and all(p.is_relative_to(self.directory.resolve()) for p in files), files
        source = files[0].parent
        assert all(p.parent == source for p in files), files
        self.server.stop()
        destination = self.directory / 'restored'
        restored = c.tool('ffkv_process', {'action': 'restore', 'source': str(source), 'data_dir': str(destination)})
        assert restored['success'] is True and restored['exit_code'] == 0, restored
        rejected = c.tool('ffkv_process', {'action': 'restore', 'source': str(source), 'data_dir': str(destination)}, error=True)
        assert rejected['success'] is False and rejected['exit_code'] != 0, rejected
        with Server(str(self.server_binary), destination, 'strict') as restored_server:
            assert restored_server.connect().command('GET', 'mcp:restore') == b'sealed'
        disabled_config = dict(self.config, process={'enabled': False})
        path = self.directory / 'disabled.json'
        path.write_text(json.dumps(disabled_config))
        disabled = self.new_mcp(config=path)
        disabled.tool('ffkv_process', {'action': 'start', 'args': ['--bind', '127.0.0.1:0']}, error=True)

    def _owned_logs(self, handle):
        result = self.mcp.tool('ffkv_process', {'action': 'logs', 'process': handle, 'max_bytes': 4096})
        return result if any('listening on ' in entry.get('text', '') for entry in result['entries']) else None

    def check_artifacts(self):
        c = self.mcp
        value = bytes(range(256)) * 2048
        assert c.command('SET', 'mcp:large', value) == b'OK'
        handle = c.tool('ffkv_execute', {'commands': [['GET', 'mcp:large']]}, resolve=False)
        assert handle['encoding'] == 'json' and handle['total_bytes'] > 4096, handle
        result = c.read_artifact(handle, page_size=16381)
        assert result['outcome'] == 'ok' and resp(result['replies'][0]) == value, result.keys()
        c.tool('ffkv_result', {'action': 'read', 'result_id': handle['result_id']}, error=True)
        # Both directions exceed socket buffers: writing the entire batch before
        # draining replies can deadlock here, unlike a small PING-only pipeline.
        commands = []
        expected = []
        for _ in range(8):
            commands.extend([['GET', 'mcp:large'], ['SET', 'mcp:large', value]])
            expected.extend([value, b'OK'])
        assert c.execute(commands) == expected
        # An interrupted pipeline must page its already received data too,
        # without embedding a large serialized result inside an error string.
        session = c.open()
        handle = c.tool('ffkv_execute', {
            'session': session,
            'commands': [['GET', 'mcp:large'], ['BLPOP', 'mcp:partial-timeout', 0]],
            'timeout_ms': 500,
        }, error=True, resolve=False)
        assert handle['encoding'] == 'json' and handle['total_bytes'] > 4096, handle
        partial = c.read_artifact(handle)
        assert partial['outcome'] == 'unknown' and partial['closed'] is True, partial.keys()
        assert [resp(frame) for frame in partial['replies']] == [value]
        c.tool('ffkv_execute', {'session': session, 'commands': [['PING']]}, error=True)

    def check_benchmark(self):
        # Exercise the actual reference transport, not a duplicate test client.
        # Twelve 1 MiB SET/GET pairs exceeded both socket buffers and deadlocked
        # the previous write-all-before-read implementation.
        result = subprocess.run([
            sys.executable, str(ROOT / 'benchmarks/mcp.py'),
            '--binary', str(self.server_binary), '--mcp-binary', str(self.mcp_binary),
            '--rounds', '1', '--warmup', '1', '--batch-sizes', '12',
            '--value-bytes', '1048576',
        ], capture_output=True, text=True, timeout=60)
        assert result.returncode == 0, result.stderr
        # The benchmark asserts every returned value before publishing timings.
        report = json.loads(result.stdout)
        assert {(row['transport'], row['workload'], row['commands']) for row in report['results']} == {
            ('mcp_stdio', 'ping', 12), ('direct_resp', 'ping', 12),
            ('mcp_stdio', 'set_get', 24), ('direct_resp', 'set_get', 24),
        }, report

    def close(self):
        for client in reversed(self.mcps):
            client.close()
        self.mcps.clear()
        for server in reversed(self.servers):
            server.cleanup()
        self.servers.clear()
        self.temporary.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', help='ffkv server binary')
    parser.add_argument('--mcp-binary', help='ffkv-mcp binary')
    parser.add_argument('--group', action='append', choices=FEATURE_COVERAGE)
    parser.add_argument('--coverage-report', type=Path)
    args = parser.parse_args()
    groups = args.group or list(FEATURE_COVERAGE)
    # The offline lifecycle group deliberately stops the fixture's server.
    groups = [g for g in groups if g != 'process'] + (['process'] if 'process' in groups else [])
    with Fixture(args.binary, args.mcp_binary) as fixture:
        for group in groups:
            fixture.run(group)
            print(f'PASS MCP {group}: {len(FEATURE_COVERAGE[group])} behavior contracts', flush=True)
        report = {'transport': 'real stdio JSON-RPC', 'completed': fixture.completed,
                  'coverage': {g: FEATURE_COVERAGE[g] for g in fixture.completed},
                  'scope': 'Representative feature behavior; not every command option or failure mode.'}
        if args.coverage_report:
            args.coverage_report.write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
