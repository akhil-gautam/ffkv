#!/usr/bin/env python3
"""Exercise all 19 Redis datatype families through ffkv-cli on owned servers.

No Redis server or client executable is required. Fixtures cover datatype
behavior, cross-shard operations, typed WAL/snapshot recovery, absolute TTLs,
and native RESP2/RESP3 decoding. SIGKILL is not a power-loss simulation.
"""
import argparse
import base64
import json
import math
from pathlib import Path
import select
import signal
import subprocess
import tempfile
import time

from client_smoke import client_env, run
from durability_smoke import Server, passed


class Cli:
    def __init__(self, binary, server):
        self.binary = binary
        self.server = server

    def call(self, *args, version=2, code=0, data=None):
        flags = [f"-{version}", "--json"]
        if data is not None:
            flags.append("-x")
        result = run(self.binary, self.server.address, *flags, *args,
                     code=code, data=data)
        return json.loads(result.stdout)

    def equal(self, expected, *args, **kwargs):
        actual = self.call(*args, **kwargs)
        assert actual == expected, (args, actual, expected)
        return actual

    def error(self, fragment, *args, **kwargs):
        actual = self.call(*args, code=1, **kwargs)
        assert actual["type"] == "error" and fragment in actual["value"], (args, actual)
        return actual


def number(value):
    if isinstance(value, dict) and value.get("type") == "double":
        return float(value["value"])
    return float(value)


def sample_rows(rows):
    return [[row[0], *[number(value) for value in row[1:]]] for row in rows]


def binary(value):
    if isinstance(value, str):
        return value.encode()
    assert value["type"] == "bytes", value
    return base64.b64decode(value["base64"], validate=True)


def dump_roundtrip(cli, family, source, destination):
    cursor = 0
    pages = 0
    while True:
        next_cursor, data = cli.call(f"{family}.SCANDUMP", source, cursor)
        if next_cursor == 0:
            break
        cli.equal("OK", f"{family}.LOADCHUNK", destination, next_cursor,
                  data=binary(data))
        pages += 1
        assert next_cursor != cursor and pages < 1000, (family, cursor, next_cursor)
        cursor = next_cursor


def seed(cli):
    # The seven existing logical families, including packed bitmap/bitfield data.
    cli.equal("OK", "SET", "types:string", "42")
    cli.equal(0, "SETBIT", "types:bitmap", 0, 1)
    cli.equal(0, "SETBIT", "types:bitmap", 9, 1)
    cli.equal([65535, 7], "BITFIELD", "types:bitfield", "OVERFLOW", "SAT",
              "INCRBY", "u16", "#0", 70000, "INCRBY", "u16", "#1", 7)
    cli.equal(2, "HSET", "types:hash", "status", "ready", "attempts", 2)
    cli.equal(3, "RPUSH", "types:list", "first", "middle", "last")
    cli.equal("first", "LPOP", "types:list")
    cli.equal(2, "SADD", "types:set", "a", "b", "a")
    cli.equal(2, "ZADD", "types:zset", 3, "third", 1, "first")

    # Sparse arrays: logical length is not occupied count, upper-half indices
    # remain unsigned on both wire protocols, and overflow must be atomic.
    cli.equal(2, "ARMSET", "types:array", 2, "two", 5, "five")
    cli.equal(["five", None, None, "two"], "ARGETRANGE", "types:array", 5, 2)
    cli.equal(1, "ARSET", "types:array:wide", 18446744073709551614, "tail")
    cli.equal("18446744073709551615", "ARLEN", "types:array:wide")
    cli.equal({"type": "big-number", "value": "18446744073709551615"},
              "ARLEN", "types:array:wide", version=3)
    cli.error("overflow", "ARSET", "types:array:wide", 18446744073709551614,
              "must-not-replace", "overflow")
    cli.equal("tail", "ARGET", "types:array:wide", 18446744073709551614)

    # Geo and HLL keep Redis's underlying zset/string representation.
    cli.equal(2, "GEOADD", "types:geo", 13.361389, 38.115556, "Palermo",
              15.087269, 37.502669, "Catania")
    distance = number(cli.call("GEODIST", "types:geo", "Palermo", "Catania", "km"))
    assert abs(distance - 166.2742) < 0.01, distance
    cli.equal(["Palermo", "Catania"], "GEOSEARCH", "types:geo", "FROMMEMBER",
              "Palermo", "BYRADIUS", 200, "km", "ASC")
    cli.equal(1, "PFADD", "types:hll", "a", "b", "c")
    cli.equal(0, "PFADD", "types:hll", "a")
    cli.equal("OK", "PFMERGE", "types:hll:merged", "types:hll")

    # JSONPath matches operate on actual document nodes, not stored text echoes.
    document = {"items": [{"name": "cheap", "price": 2},
                           {"name": "costly", "price": 9}], "enabled": True}
    cli.equal("OK", "JSON.SET", "types:json", "$", json.dumps(document))
    cli.equal("[\"costly\"]", "JSON.GET", "types:json", "$.items[?(@.price > 5)].name")
    cli.equal("[5,12]", "JSON.NUMINCRBY", "types:json", "$.items[*].price", 3)
    cli.equal([False], "JSON.TOGGLE", "types:json", "$.enabled", version=3)
    cli.equal("OK", "JSON.MSET", "types:json:a", "$", '{"v":1}',
              "types:json:b", "$", '{"v":2}')
    cli.error("new objects", "JSON.MSET", "types:json:a", "$.v", "99",
              "types:json:missing", "$.child", "1")
    cli.equal("[1]", "JSON.GET", "types:json:a", "$.v")

    # Stream payload order/repeated fields and durable PEL state.
    cli.equal("1000-0", "XADD", "types:stream", "1000-0", "field", "a", "field", "b")
    cli.equal("1001-0", "XADD", "types:stream", "1001-0", "field", "c")
    cli.equal("OK", "XGROUP", "CREATE", "types:stream", "workers", 0)
    cli.equal([["types:stream", [["1000-0", ["field", "a", "field", "b"]]]]],
              "XREADGROUP", "GROUP", "workers", "alice", "COUNT", 1,
              "STREAMS", "types:stream", ">")

    # Rules are live: crossing a bucket emits its aggregate, LATEST exposes the
    # still-open bucket without materializing it as an ordinary destination row.
    cli.equal("OK", "TS.CREATE", "types:ts", "LABELS", "sensor", "room")
    cli.equal("OK", "TS.CREATE", "types:ts:avg")
    cli.equal("OK", "TS.CREATERULE", "types:ts", "types:ts:avg",
              "AGGREGATION", "avg", 100)
    for timestamp, value in ((20, 2), (80, 4), (150, 8)):
        cli.equal(timestamp, "TS.ADD", "types:ts", timestamp, value)
    assert sample_rows(cli.call("TS.RANGE", "types:ts:avg", "-", "+")) == [[0, 3.0]]
    assert sample_rows([cli.call("TS.GET", "types:ts:avg", "LATEST")]) == [[100, 8.0]]
    partial = cli.call("TS.MADD", "types:ts", 160, 10,
                       "types:ts:missing", 160, 1, code=1)
    assert partial[0] == 160 and partial[1]["type"] == "error", partial
    cli.equal("OK", "TS.CREATE", "types:ts:nan")
    cli.equal(1, "TS.ADD", "types:ts:nan", 1, "nan")
    assert math.isnan(number(cli.call("TS.GET", "types:ts:nan", version=3)[1]))

    # Actual bounded membership structures and chunked binary export/import.
    cli.equal("OK", "BF.RESERVE", "types:bloom", 0.001, 100000)
    cli.equal([1, 1], "BF.MADD", "types:bloom", "alpha", "beta")
    cli.equal(0, "BF.ADD", "types:bloom", "alpha")
    dump_roundtrip(cli, "BF", "types:bloom", "types:bloom:loaded")
    cli.equal("OK", "CF.RESERVE", "types:cuckoo", 128)
    cli.equal(1, "CF.ADD", "types:cuckoo", "duplicate")
    cli.equal(1, "CF.ADD", "types:cuckoo", "duplicate")
    cli.equal(2, "CF.COUNT", "types:cuckoo", "duplicate")
    cli.equal(1, "CF.DEL", "types:cuckoo", "duplicate")
    dump_roundtrip(cli, "CF", "types:cuckoo", "types:cuckoo:loaded")

    # Weighted multi-key CMS merge and the retained HeavyKeeper top-K heap.
    for key in ("types:cms", "types:cms:other", "types:cms:merged"):
        cli.equal("OK", "CMS.INITBYDIM", key, 128, 4)
    cli.equal([3, 5], "CMS.INCRBY", "types:cms", "alpha", 3, "beta", 5)
    cli.equal([2], "CMS.INCRBY", "types:cms:other", "alpha", 2)
    cli.equal("OK", "CMS.MERGE", "types:cms:merged", 2, "types:cms",
              "types:cms:other", "WEIGHTS", 2, 3)
    cli.equal("OK", "TOPK.RESERVE", "types:topk", 2, 64, 5, 0.9)
    cli.equal([None, None], "TOPK.INCRBY", "types:topk", "alpha", 20, "beta", 1)
    cli.equal("OK", "TDIGEST.CREATE", "types:tdigest")
    cli.equal("OK", "TDIGEST.ADD", "types:tdigest", 10, 10, 10, 10, 20, 20)
    cli.equal("OK", "TDIGEST.MERGE", "types:tdigest:merged", 1, "types:tdigest")

    # HNSW nearest-neighbor queries and attribute filtering.
    cli.equal(1, "VADD", "types:vector", "VALUES", 3, 1, 0, 0, "east",
              "NOQUANT", "SETATTR", '{"zone":"east"}')
    cli.equal(1, "VADD", "types:vector", "VALUES", 3, 0, 1, 0, "north",
              "NOQUANT", "SETATTR", '{"zone":"north"}')
    cli.equal(["east"], "VSIM", "types:vector", "VALUES", 3, 1, 0, 0,
              "COUNT", 1)
    cli.equal(["north"], "VSIM", "types:vector", "ELE", "east",
              "FILTER", '.zone == "north"', "COUNT", 1, "FILTER-EF", 0)
    scored = cli.call("VSIM", "types:vector", "ELE", "east", "COUNT", 1,
                      "WITHSCORES", "WITHATTRIBS", version=3)
    assert list(scored) == ["east"] and abs(scored["east"][0] - 1) < 1e-7, scored
    assert json.loads(scored["east"][1]) == {"zone": "east"}, scored
    cli.error("invalid filter operator", "VSIM", "types:vector", "VALUES", 3, 1, 0, 0,
              "FILTER", "é")
    cli.equal("PONG", "PING")


def verify(cli):
    cli.equal("42", "GET", "types:string")
    cli.equal(2, "BITCOUNT", "types:bitmap")
    cli.equal([65535, 7], "BITFIELD_RO", "types:bitfield", "GET", "u16", "#0",
              "GET", "u16", "#1")
    cli.equal(["ready", "2"], "HMGET", "types:hash", "status", "attempts")
    cli.equal(["middle", "last"], "LRANGE", "types:list", 0, -1)
    assert set(cli.call("SMEMBERS", "types:set")) == {"a", "b"}
    cli.equal(["first", "third"], "ZRANGE", "types:zset", 0, -1)
    cli.equal(["two", None, None, "five"], "ARGETRANGE", "types:array", 2, 5)
    cli.equal("tail", "ARGET", "types:array:wide", 18446744073709551614)
    cli.equal(["sqc8b49rny0", "sqdtr74hyu0"], "GEOHASH", "types:geo", "Palermo", "Catania")
    cli.equal(3, "PFCOUNT", "types:hll:merged")
    cli.equal("[5,12]", "JSON.GET", "types:json", "$.items[*].price")
    cli.equal("[false]", "JSON.GET", "types:json", "$.enabled")
    cli.equal([1, "1000-0", "1000-0", [["alice", "1"]]],
              "XPENDING", "types:stream", "workers")
    cli.equal([["1000-0", ["field", "a", "field", "b"]],
               ["1001-0", ["field", "c"]]], "XRANGE", "types:stream", "-", "+")
    assert sample_rows(cli.call("TS.RANGE", "types:ts", "-", "+")) == [
        [20, 2.0], [80, 4.0], [150, 8.0], [160, 10.0]]
    assert sample_rows(cli.call("TS.RANGE", "types:ts:avg", "-", "+")) == [[0, 3.0]]
    assert sample_rows([cli.call("TS.GET", "types:ts:avg", "LATEST")]) == [[100, 9.0]]
    assert math.isnan(number(cli.call("TS.GET", "types:ts:nan", version=3)[1]))
    cli.equal(["types:ts"], "TS.QUERYINDEX", "sensor=room")
    for key in ("types:bloom", "types:bloom:loaded"):
        cli.equal([True, True], "BF.MEXISTS", key, "alpha", "beta", version=3)
    for key in ("types:cuckoo", "types:cuckoo:loaded"):
        cli.equal(1, "CF.COUNT", key, "duplicate")
    cli.equal([12, 10], "CMS.QUERY", "types:cms:merged", "alpha", "beta")
    cli.equal(["alpha", 20, "beta", 1], "TOPK.LIST", "types:topk", "WITHCOUNT")
    cli.equal([2, 5], "TDIGEST.RANK", "types:tdigest:merged", 10, 20)
    assert [number(x) for x in cli.call("TDIGEST.QUANTILE", "types:tdigest", 0, 1)] == [10, 20]
    cli.equal(2, "VCARD", "types:vector")
    cli.equal(["east"], "VSIM", "types:vector", "ELE", "east", "COUNT", 1)
    assert json.loads(cli.call("VGETATTR", "types:vector", "north")) == {"zone": "north"}


def memory_and_waits(server_binary, cli_binary):
    with Server(server_binary, extra_args=("--shards", "4")) as server:
        cli = Cli(cli_binary, server)
        seed(cli)
        verify(cli)
        control = server.connect()
        children = []

        def wait_count(expected):
            deadline = time.monotonic() + 5
            while True:
                fields = dict(line.split(b":", 1) for line in
                              control.command("INFO", "clients").splitlines()
                              if b":" in line)
                count = int(fields[b"blocked_clients"])
                if count == expected:
                    return
                assert time.monotonic() < deadline, ("blocked clients", count, expected)
                time.sleep(0.01)

        def waiting(*args):
            child = subprocess.Popen(
                [cli_binary, "-h", server.address[0], "-p", str(server.address[1]),
                 "-2", "--json", *map(str, args)],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, env=client_env())
            children.append(child)
            wait_count(1)
            assert child.poll() is None, (args, child.communicate())
            return child

        def finished(child, expected=None, code=0):
            stdout, stderr = child.communicate(timeout=5)
            assert child.returncode == code, (child.returncode, stdout, stderr)
            actual = json.loads(stdout)
            if expected is not None:
                assert actual == expected, (actual, expected)
            wait_count(0)
            return actual

        try:
            cli.equal("OK", "XGROUP", "CREATE", "wait:stream", "workers", 0, "MKSTREAM")
            child = waiting("XREADGROUP", "GROUP", "workers", "bob", "BLOCK", 0,
                            "STREAMS", "wait:stream", ">")
            entry_id = cli.call("XADD", "wait:stream", "*", "job", "one")
            finished(child, [["wait:stream", [[entry_id, ["job", "one"]]]]])
            cli.equal(1, "XACK", "wait:stream", "workers", entry_id)

            # Group deletion and key expiry must wake blocked group readers
            # with an error, rather than leak indefinitely parked clients.
            child = waiting("XREADGROUP", "GROUP", "workers", "bob", "BLOCK", 0,
                            "STREAMS", "wait:stream", ">")
            cli.equal(1, "XGROUP", "DESTROY", "wait:stream", "workers")
            assert finished(child, code=1)["type"] == "error"
            cli.equal("OK", "XGROUP", "CREATE", "wait:expiry", "workers", 0, "MKSTREAM")
            child = waiting("XREADGROUP", "GROUP", "workers", "bob", "BLOCK", 0,
                            "STREAMS", "wait:expiry", ">")
            cli.equal(1, "PEXPIRE", "wait:expiry", 300)
            assert finished(child, code=1)["type"] == "error"

            cli.equal("OK", "TS.CREATE", "wait:ts")
            child = waiting("TS.READ", "wait:ts", "$", "BLOCK", 0, 2)
            cli.equal(10, "TS.ADD", "wait:ts", 10, 1)
            assert not select.select([child.stdout], [], [], 0.05)[0], "TS.READ ignored min_count"
            cli.equal(20, "TS.ADD", "wait:ts", 20, 2)
            assert sample_rows(finished(child)) == [[10, 1.0], [20, 2.0]]
            child = waiting("TS.READ", "wait:ts", "$", "BLOCK", 500, 2)
            cli.equal(30, "TS.ADD", "wait:ts", 30, 3)
            assert sample_rows(finished(child)) == [[30, 3.0]]

            child = waiting("XREAD", "BLOCK", 0, "STREAMS", "wait:cancel", "$")
            child.terminate()
            child.communicate(timeout=5)
            wait_count(0)
            cli.equal("1-0", "XADD", "wait:cancel", "1-0", "v", "not-stolen")
            cli.equal([["1-0", ["v", "not-stolen"]]], "XRANGE", "wait:cancel", "-", "+")
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.wait(timeout=5)
                child.stdout.close()
                child.stderr.close()
        passed("19 memory-only families; native blocked group wake/delete/expiry, TS minimum/partial timeout, EOF cancellation")


def recovery(server_binary, cli_binary, storage):
    with tempfile.TemporaryDirectory(prefix=f"ffkv-datatypes-{storage}-") as directory:
        data = Path(directory) / "data"
        with Server(server_binary, data, "strict",
                    extra_args=("--storage", storage, "--shards", "4")) as server:
            cli = Cli(cli_binary, server)
            seed(cli)
            verify(cli)
            # Absolute deadlines must not restart their duration on replay/export.
            cli.equal(1, "PEXPIRE", "types:array", 600000)
            expires = cli.call("PEXPIRETIME", "types:array")
            server.stop(signal.SIGKILL)
            server.extra_args = ("--storage", storage, "--shards", "1")
            server.start()
            verify(cli)
            cli.equal(expires, "PEXPIRETIME", "types:array")
            cli.call("FFKV.CHECKPOINT")
            cli.equal("1002-0", "XADD", "types:stream:post-checkpoint", "1002-0", "v", "after")
            cli.call("FFKV.BACKUP", "all-types")
            server.stop(signal.SIGKILL)
            server.extra_args = ("--storage", storage, "--shards", "4")
            server.start()
            verify(cli)
            cli.equal([["1002-0", ["v", "after"]]], "XRANGE",
                      "types:stream:post-checkpoint", "-", "+")
            cli.equal(expires, "PEXPIRETIME", "types:array")
        restored = Path(directory) / "restored"
        result = subprocess.run([server_binary, "restore", "--source",
                                 str(data / "backups" / "all-types"),
                                 "--data-dir", str(restored)],
                                capture_output=True, env=client_env(), timeout=30)
        assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
        with Server(server_binary, restored, "strict",
                    extra_args=("--storage", storage, "--shards", "2")) as server:
            cli = Cli(cli_binary, server)
            verify(cli)
            cli.equal(expires, "PEXPIRETIME", "types:array")
        passed(f"19 families through native CLI: {storage}, WAL crash recovery 4->1, checkpoint 1->4, backup restore ->2")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--client", default="target/debug/ffkv-cli")
    args = parser.parse_args()
    if not __debug__:
        parser.error("run without -O: assertions enforce datatype invariants")
    memory_and_waits(args.binary, args.client)
    for storage in ("memory", "hybrid"):
        recovery(args.binary, args.client, storage)


if __name__ == "__main__":
    main()
