#!/usr/bin/env python3
"""Exercise string/bitmap commands, optional Redis comparison, and strict recovery.

Usage: python3 tests/strings_smoke.py [--binary target/debug/ffkv]
       python3 tests/strings_smoke.py --reference 127.0.0.1:16380
Only owned ffkv:test:strings: keys are touched on the optional reference.
The 8 MiB ffkv logical value limit is tested separately from Redis semantics.
"""
import argparse
import concurrent.futures
import random
import signal
import tempfile
import threading

from cli_smoke import Client
from durability_smoke import Server


PREFIX = b"ffkv:test:strings:"
LIMIT = 8 * 1024 * 1024


def key(suffix):
    return PREFIX + suffix.encode()


def is_error(reply):
    return isinstance(reply, tuple) and reply[0] == "error"


def category(reply):
    if is_error(reply):
        return ("error", reply[1].split(b" ", 1)[0])
    if isinstance(reply, list):
        return [category(item) for item in reply]
    return reply


def exercise(client, reference=None):
    keys = [key(suffix) for suffix in ["value", "other", "missing", "list", "metrics", "a", "b", "bits", "empty"]]
    client.command("DEL", *keys)
    if reference:
        reference.command("DEL", *keys)
    checks = 0

    def check(args, expected):
        nonlocal checks
        actual = client.command(*args)
        assert actual == expected, (args, actual, expected)
        if reference:
            other = reference.command(*args)
            assert category(other) == category(actual), (args, actual, other)
        checks += 1
        return actual

    def failure(args, expected_code=b"ERR"):
        nonlocal checks
        actual = client.command(*args)
        assert is_error(actual) and actual[1].split(b" ", 1)[0] == expected_code, (args, actual)
        if reference:
            other = reference.command(*args)
            assert category(other) == category(actual), (args, actual, other)
        checks += 1

    k, other, missing, linked, metrics, a, b, bits, empty = keys
    check(["SETNX", k, b"a\x00b"], 1)
    check(["SETNX", k, b"changed"], 0)
    check(["STRLEN", k], 3)
    check(["APPEND", k, b"\xff"], 4)
    check(["GETRANGE", k, 1, -1], b"\x00b\xff")
    check(["GETRANGE", k, -(2**63), 2**63 - 1], b"a\x00b\xff")
    check(["GETRANGE", k, 0, -(2**63)], b"a")
    check(["SETRANGE", k, 6, b"xy"], 8)
    check(["GET", k], b"a\x00b\xff\x00\x00xy")
    check(["SETRANGE", missing, 2**63 - 1, b""], 0)
    check(["EXISTS", missing], 0)
    check(["APPEND", empty, b""], 0)
    check(["EXISTS", empty], 1)
    check(["GETSET", k, b"replacement"], b"a\x00b\xff\x00\x00xy")
    check(["GETDEL", k], b"replacement")
    check(["GETDEL", k], None)
    check(["SETEX", k, 60, b"ttl"], b"OK")
    assert 0 < client.command("PTTL", k) <= 60000
    check(["APPEND", k, "x"], 4)
    check(["SETRANGE", k, 0, "T"], 4)
    check(["SETBIT", k, 0, 0], 0)
    check(["BITFIELD", k, "SET", "u1", 0, 0], [0])
    assert 0 < client.command("PTTL", k) <= 60000
    check(["GETEX", k, "PERSIST"], b"Ttlx")
    assert client.command("PTTL", k) == -1
    check(["PSETEX", k, 60000, "new"], b"OK")
    check(["GETEX", k, "EX", 60], b"new")
    check(["GETEX", k, "PX", 60000], b"new")
    check(["GETEX", k, "EXAT", 1], b"new")
    check(["GET", k], None)
    check(["SET", k, "new"], b"OK")
    check(["GETEX", k, "PXAT", 1], b"new")
    check(["GETEX", missing, "EX", "invalid"], None)
    check(["SET", k, "unchanged"], b"OK")
    failure(["GETEX", k, "EX", 0])
    failure(["GETEX", k, "EX", 60, "PERSIST"])
    failure(["PSETEX", k, 0, "bad"])
    check(["GET", k], b"unchanged")
    check(["LPUSH", linked, "x"], 1)
    failure(["GETEX", linked, "EX", "not-an-int"], b"WRONGTYPE")
    for cmd in [["GETSET", linked, "x"], ["GETDEL", linked], ["APPEND", linked, "x"], ["STRLEN", linked], ["SETRANGE", linked, 0, ""], ["BITCOUNT", linked], ["BITPOS", linked, 0], ["BITFIELD", linked, "GET", "u8", 0]]:
        failure(cmd, b"WRONGTYPE")
    failure(["SETBIT", linked, "invalid", 2])
    failure(["BITCOUNT", linked, "invalid", 0])
    failure(["BITFIELD", linked, "SET", "u64", 0, 1])

    check(["MSETNX", a, "one", b, "two"], 1)
    check(["MSETNX", missing, "must-not-exist", a, "wrong"], 0)
    check(["MGET", a, b, missing], [b"one", b"two", None])
    check(["MSETNX", missing, "first", missing, "last"], 1)
    check(["GET", missing], b"last")
    failure(["MSETNX", a, "wrong", b])
    check(["GET", a], b"one")
    check(["GETBIT", bits, 0], 0)
    check(["SETBIT", bits, 0, 1], 0)
    check(["SETBIT", bits, 7, 1], 0)
    check(["SETBIT", bits, 8, 1], 0)
    check(["GET", bits], b"\x81\x80")
    check(["GETBIT", bits, 7], 1)
    check(["GETBIT", bits, 1000], 0)
    check(["BITCOUNT", bits], 3)
    check(["BITCOUNT", bits, 1, 1, "BYTE"], 1)
    check(["BITCOUNT", bits, 1, 7, "BIT"], 1)
    check(["BITCOUNT", bits, -1, -1, "BIT"], 0)
    check(["BITCOUNT", bits, -(2**63), 2**63 - 1, "BIT"], 3)
    check(["BITCOUNT", bits, 0, -(2**63)], 2)
    check(["BITCOUNT", bits, -5, -6], 0)
    check(["GETRANGE", bits, -5, -6], b"")
    check(["BITPOS", bits, 1, -5, -6], 0)
    check(["BITPOS", bits, 0], 1)
    check(["BITPOS", bits, 1, 1, 1, "BYTE"], 8)
    check(["BITPOS", bits, 1, 1, 7, "BIT"], 7)
    check(["BITPOS", bits, 1, 9, 100, "BIT"], -1)
    check(["SET", other, b"\xff\xff"], b"OK")
    check(["BITPOS", other, 0], 16)
    check(["BITPOS", other, 0, 0, 1], -1)
    check(["BITPOS", other, 0, 2], -1)
    check(["DEL", missing], 1)
    check(["BITPOS", missing, 0, 100, 101, "BIT"], 0)
    check(["BITPOS", empty, 0], -1)
    failure(["BITCOUNT", bits, 0])
    failure(["BITCOUNT", bits, 0, 1, "INVALID"])
    failure(["BITPOS", bits, 2])
    failure(["SETBIT", bits, 0, 2])
    failure(["BITOP", "NOT", k, bits, other])
    check(["BITOP", "AND", k, bits, other], 2)
    check(["GET", k], b"\x81\x80")
    check(["BITOP", "XOR", k, k, other], 2)
    check(["GET", k], b"\x7e\x7f")
    check(["BITOP", "OR", k, k, bits], 2)
    check(["GET", k], b"\xff\xff")
    check(["BITOP", "NOT", k, k], 2)
    check(["GET", k], b"\x00\x00")
    failure(["BITOP", "OR", k, bits, linked], b"WRONGTYPE")
    check(["GET", k], b"\x00\x00")
    check(["BITOP", "AND", k, bits, missing], 2)
    check(["GET", k], b"\x00\x00")
    check(["BITOP", "OR", k, missing, empty], 0)
    check(["EXISTS", k], 0)

    # Exact Sidekiq bucket encoding: multiple saturating unsigned 16-bit counters.
    check(["BITFIELD", metrics, "OVERFLOW", "SAT", "INCRBY", "u16", "#0", 65534, "INCRBY", "u16", "#1", 3, "INCRBY", "u16", "#0", 10], [65534, 3, 65535])
    check(["GET", metrics], b"\xff\xff\x00\x03")
    check(["BITFIELD_RO", metrics, "GET", "u16", "#0", "GET", "u16", "#1"], [65535, 3])
    failure(["BITFIELD_RO", metrics, "SET", "u16", "#0", 0])
    failure(["BITFIELD", metrics, "SET", "u8", 0, 0, "GET", "u64", 0])
    check(["GET", metrics], b"\xff\xff\x00\x03")
    check(["BITFIELD", metrics, "OVERFLOW", "FAIL", "INCRBY", "u16", "#0", 1, "GET", "u16", "#0", "INCRBY", "u16", "#1", 1], [None, 65535, 4])
    check(["BITFIELD", missing, "OVERFLOW", "FAIL", "SET", "u1", 0, 2, "GET", "u1", 0], [None, 0])
    check(["EXISTS", missing], 1)
    check(["GET", missing], b"\x00")
    check(["DEL", missing], 1)
    check(["WATCH", missing], b"OK")
    check(["BITFIELD", missing, "OVERFLOW", "FAIL", "SET", "u1", 64, 2], [None])
    check(["GET", missing], b"\x00" * 9)
    check(["MULTI"], b"OK")
    check(["PING"], b"QUEUED")
    check(["EXEC"], [b"PONG"])
    check(["BITFIELD", metrics, "SET", "i64", 0, 2**63 - 1, "INCRBY", "i64", 0, 1, "GET", "i64", 0], [0xFFFF000400000000 - 2**64, -(2**63), -(2**63)])
    check(["BITFIELD", metrics, "OVERFLOW", "SAT", "INCRBY", "i64", 0, -1], [-(2**63)])
    check(["BITFIELD", metrics, "OVERFLOW", "FAIL", "INCRBY", "i64", 0, -1, "GET", "i64", 0], [None, -(2**63)])

    # Width/unaligned byte boundaries, sign extension, and all overflow modes.
    rng = random.Random(74519)
    for signed, width in [(False, 1), (False, 7), (False, 8), (False, 9), (False, 16), (False, 63), (True, 1), (True, 8), (True, 31), (True, 64)]:
        encoding = ("i" if signed else "u") + str(width)
        low = -(2 ** (width - 1)) if signed else 0
        high = 2 ** (width - (1 if signed else 0)) - 1
        for mode in ["WRAP", "SAT", "FAIL"]:
            for _ in range(4):
                offset = rng.randrange(0, 25)
                initial = rng.randint(low, high)
                delta = rng.choice([-1, 1, -(2**63), 2**63 - 1])
                check(["DEL", bits], client.command("EXISTS", bits))
                check(["BITFIELD", bits, "SET", encoding, offset, initial], [0])
                desired = initial + delta
                if low <= desired <= high:
                    expected = desired
                elif mode == "FAIL":
                    expected = None
                elif mode == "SAT":
                    expected = min(high, max(low, desired))
                else:
                    expected = desired % (2**width)
                    if signed and expected > high:
                        expected -= 2**width
                check(["BITFIELD", bits, "OVERFLOW", mode, "INCRBY", encoding, offset, delta, "GET", encoding, offset], [expected, initial if expected is None else expected])
    print(f"PASS {checks} string/bitmap assertions and optional Redis differential", flush=True)


def limits(client):
    k = key("limits")
    client.command("SET", k, "unchanged")
    for args in [["SETBIT", k, LIMIT * 8, 1], ["GETBIT", k, LIMIT * 8], ["SETRANGE", k, LIMIT, "x"], ["BITFIELD", k, "SET", "i64", LIMIT * 8 - 63, 1], ["BITFIELD", k, "GET", "u8", "#9223372036854775807"]]:
        response = client.command(*args)
        assert is_error(response), (args[:3], response)
        assert client.command("GET", k) == b"unchanged"
    # Both requests fit the framing cap; their combined value exceeds the
    # independent stored-value cap. An oversized frame instead closes TCP.
    seed = b"x" * 4096
    assert client.command("SET", k, seed) == b"OK"
    assert is_error(client.command("APPEND", k, b"y" * (LIMIT - 2048)))
    assert client.command("GET", k) == seed
    assert client.command("SETBIT", k, LIMIT * 8 - 1, 1) == 0
    assert client.command("STRLEN", k) == LIMIT
    assert client.command("GETBIT", k, LIMIT * 8 - 1) == 1
    assert client.command("BITCOUNT", k, LIMIT - 1, LIMIT - 1) == 1
    client.command("DEL", k)
    print("PASS exact 8 MiB boundary and nonmutating oversized writes", flush=True)


def atomic_race(server):
    a, b = key("race:a"), key("race:b")
    observer = server.connect()
    clients = [server.connect(), server.connect()]
    for attempt in range(20):
        observer.command("DEL", a, b)
        gate = threading.Barrier(2)

        def contend(index):
            gate.wait(timeout=5)
            return clients[index].command("MSETNX", a, f"{attempt}:{index}", b, f"{attempt}:{index}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(contend, range(2)))
        assert sorted(results) == [0, 1], results
        winner = results.index(1)
        value = f"{attempt}:{winner}".encode()
        assert observer.command("MGET", a, b) == [value, value]
    print("PASS contending all-or-none MSETNX pairs", flush=True)


def recovery(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-string-recovery-") as directory:
        with Server(binary, directory, durability="strict") as server:
            client = server.connect()
            bits, metrics, a, b, deleted = [key(suffix) for suffix in ["recover:bits", "recover:metrics", "recover:a", "recover:b", "recover:deleted"]]
            assert client.command("SETBIT", bits, 65535, 1) == 0
            assert client.command("SETBIT", bits, 0, 1) == 0
            assert client.command("PEXPIRE", bits, 600000) == 1
            assert client.command("BITFIELD", metrics, "OVERFLOW", "SAT", "INCRBY", "u16", "#0", 65540, "INCRBY", "u16", "#3", 9) == [65535, 9]
            assert client.command("BITFIELD", metrics, "OVERFLOW", "FAIL", "INCRBY", "u16", "#0", 1, "INCRBY", "u16", "#3", 1) == [None, 10]
            assert client.command("MSETNX", a, b"left\x00", b, b"right\xff") == 1
            assert client.command("MSETNX", a, "wrong", deleted, "must-not-exist") == 0
            assert client.command("SET", deleted, "remove-me") == b"OK"
            assert client.command("GETDEL", deleted) == b"remove-me"
            expected_bits = b"\x80" + bytes(8190) + b"\x01"
            expected_metrics = b"\xff\xff\x00\x00\x00\x00\x00\x0a"
            for _ in range(2):
                server.stop(signal.SIGKILL)
                server.start()
                client = server.connect()
                assert client.command("GET", bits) == expected_bits
                assert 0 < client.command("PTTL", bits) <= 600000
                assert client.command("GET", metrics) == expected_metrics
                assert client.command("MGET", a, b, deleted) == [b"left\x00", b"right\xff", None]
                assert client.command("BITCOUNT", bits) == 2
                assert client.command("BITFIELD_RO", metrics, "GET", "u16", "#0", "GET", "u16", "#3") == [65535, 10]
    print("PASS strict SIGKILL/reopen bitmap, bitfield TTL, deletes, and atomic MSETNX state", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference", metavar="HOST:PORT")
    options = parser.parse_args()
    reference = None
    try:
        if options.reference:
            host, port = options.reference.rsplit(":", 1)
            reference = Client((host, int(port)))
        with Server(options.binary) as server:
            exercise(server.connect(), reference)
            limits(server.connect())
            atomic_race(server)
        recovery(options.binary)
    finally:
        if reference:
            reference.close()


if __name__ == "__main__":
    main()
