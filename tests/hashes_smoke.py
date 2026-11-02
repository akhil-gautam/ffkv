#!/usr/bin/env python3
"""Exercise binary hashes, Redis 8 field TTLs, mutation-safe scans, and crash recovery.

Usage: python3 tests/hashes_smoke.py [--binary target/debug/ffkv]
       python3 tests/hashes_smoke.py --reference 127.0.0.1:16380
The optional Redis 8 reference is touched only at ffkv:test:hashes: keys.

Field syntax exercised:
  HSETEX key [FNX|FXX] [EX n|PX n|EXAT n|PXAT n|KEEPTTL]
    FIELDS count field value [field value ...]
  HEXPIRE|HPEXPIRE|HEXPIREAT|HPEXPIREAT key time [NX|XX|GT|LT]
    FIELDS count field [field ...]
  HTTL|HPTTL|HEXPIRETIME|HPEXPIRETIME|HPERSIST key
    FIELDS count field [field ...]
  HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
SIGKILL recovery tests process crashes, not power loss or HA.
"""
import argparse
from collections import Counter
import signal
import tempfile
import time

from cli_smoke import Client
from durability_smoke import Server


PREFIX = b"ffkv:test:hashes:"


def check(client, args, expected):
    actual = client.command(*args)
    assert actual == expected, (args, actual, expected)
    return actual


def check_error(client, args):
    actual = client.command(*args)
    assert isinstance(actual, tuple) and actual[0] == "error", (args, actual)


def pairs(reply):
    assert isinstance(reply, list) and len(reply) % 2 == 0, reply
    result = dict(zip(reply[::2], reply[1::2]))
    assert len(result) * 2 == len(reply), ("duplicate fields in response", reply)
    return result


def wait_until_missing(client, key, field, seconds=3):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if client.command("HGET", key, field) is None:
            return
        time.sleep(0.01)
    raise AssertionError(("field did not expire", key, field))


def exercise(client):
    key = PREFIX + b"binary\x00\xff\r\n"
    wrong = PREFIX + b"wrong"
    last = PREFIX + b"last"
    all_keys = [key, wrong, last]
    client.command("DEL", *all_keys)
    try:
        field, payload = b"\x00\xff\r\n", b"\x00\xff\r\n$3\r\nabc"
        check(client, ["HSET", key, field, b"", field, payload, b"", b""], 2)
        check(client, ["HGET", key, field], payload)
        check(client, ["HMGET", key, b"missing", field, field, b""], [None, payload, payload, b""])
        check(client, ["HSTRLEN", key, field], len(payload))
        check(client, ["HSTRLEN", key, "missing"], 0)
        check(client, ["HMSET", key, "a", "1", "b", "2"], b"OK")
        check(client, ["HSETNX", key, "a", "ignored"], 0)
        check(client, ["HSETNX", key, "c", "3"], 1)
        check(client, ["HEXISTS", key, "a"], 1)
        check(client, ["HEXISTS", key, "missing"], 0)
        check(client, ["HLEN", key], 5)
        expected = {field: payload, b"": b"", b"a": b"1", b"b": b"2", b"c": b"3"}
        assert pairs(client.command("HGETALL", key)) == expected
        assert set(client.command("HKEYS", key)) == set(expected)
        assert Counter(client.command("HVALS", key)) == Counter(expected.values())
        check(client, ["HDEL", key, "c", "c", "missing"], 1)
        check(client, ["PEXPIRE", key, 60000], 1)

        # Full-field-set conditions are checked before writing even the first pair.
        check(client, ["HSETEX", key, "FNX", "PX", 5000, "FIELDS", 2,
                       "fresh", "bad", "a", "changed"], 0)
        check(client, ["HMGET", key, "fresh", "a"], [None, b"1"])
        check(client, ["HSETEX", key, "FXX", "FIELDS", 2,
                       "a", "changed", "fresh", "bad"], 0)
        check(client, ["HMGET", key, "fresh", "a"], [None, b"1"])
        check(client, ["HSETEX", key, "FNX", "FIELDS", 2,
                       "fresh", "first", "fresh", "last"], 1)
        check(client, ["HGET", key, "fresh"], b"last")
        absolute = int(time.time() * 1000) + 30000
        check(client, ["HSETEX", key, "FXX", "PXAT", absolute, "FIELDS", 2,
                       "a", "10", "fresh", "value"], 1)
        check(client, ["HPEXPIRETIME", key, "FIELDS", 3, "a", "fresh", "absent"],
              [absolute, absolute, -2])
        check(client, ["HINCRBY", key, "a", 2], 12)
        check(client, ["HSETEX", key, "KEEPTTL", "FIELDS", 1, "fresh", "kept"], 1)
        check(client, ["HPEXPIRETIME", key, "FIELDS", 2, "a", "fresh"], [absolute, absolute])
        check(client, ["HSET", key, "fresh", "persistent"], 0)
        check(client, ["HPTTL", key, "FIELDS", 1, "fresh"], [-1])
        assert 0 < client.command("PTTL", key) <= 60000

        # A persistent field behaves as infinity for GT/LT; comparisons are strict.
        check(client, ["HPEXPIREAT", key, absolute, "GT", "FIELDS", 1, "fresh"], [0])
        check(client, ["HPEXPIREAT", key, absolute, "LT", "FIELDS", 1, "fresh"], [1])
        check(client, ["HPEXPIREAT", key, absolute, "GT", "FIELDS", 1, "fresh"], [0])
        check(client, ["HPEXPIREAT", key, absolute + 1000, "GT", "FIELDS", 1, "fresh"], [1])
        check(client, ["HPEXPIREAT", key, absolute, "LT", "FIELDS", 1, "fresh"], [1])
        check(client, ["HPEXPIRE", key, 10000, "NX", "FIELDS", 3, "fresh", "b", "b"], [0, 1, 0])
        check(client, ["HPERSIST", key, "FIELDS", 3, "b", "b", "missing"], [1, -1, -2])
        check(client, ["HPEXPIRE", key, 0, "XX", "FIELDS", 2, "b", "fresh"], [0, 2])
        check(client, ["HPEXPIRE", key, 0, "FIELDS", 2, "fresh", "absent"], [-2, -2])
        check(client, ["HEXPIRE", key, 30, "FIELDS", 1, "b"], [1])
        ttl = client.command("HTTL", key, "FIELDS", 1, "b")[0]
        assert 0 < ttl <= 30, ttl
        seconds = (absolute + 999) // 1000
        check(client, ["HEXPIREAT", key, seconds, "FIELDS", 1, "b"], [1])
        check(client, ["HEXPIRETIME", key, "FIELDS", 1, "b"], [seconds])
        check(client, ["HSETEX", key, "EX", 30, "FIELDS", 1, "relative", "v"], 1)
        check(client, ["HSETEX", key, "EXAT", seconds, "FIELDS", 1, "unix", "v"], 1)
        check(client, ["HPEXPIRETIME", key, "FIELDS", 1, "unix"], [seconds * 1000])
        check(client, ["HSETEX", key, "PX", 0, "FIELDS", 1, "unix", "gone"], 1)
        check(client, ["HGET", key, "unix"], None)

        # Errors must leave the numeric value, field TTL and key TTL intact.
        check(client, ["HSETEX", key, "PXAT", absolute, "FIELDS", 1,
                       "n", 9223372036854775807], 1)
        invalid = [
            ["HINCRBY", key, "n", 1],
            ["HINCRBY", key, "n", "01"],
            ["HSET", key, "n", "bad", "dangling"],
            ["HSETEX", key, "EX", 1, "KEEPTTL", "FIELDS", 1, "n", "bad"],
            ["HSETEX", key, "FXX", "FNX", "FIELDS", 1, "n", "bad"],
            ["HSETEX", key, "FIELDS", 2, "n", "bad"],
            ["HPEXPIRE", key, -1, "FIELDS", 1, "n"],
            ["HPEXPIREAT", key, 1 << 46, "FIELDS", 1, "n"],
            ["HPEXPIRE", key, 1, "XX", "LT", "FIELDS", 1, "n"],
            ["HPERSIST", key, "FIELDS", 0],
            ["HSCAN", key, 0, "COUNT", 0],
        ]
        for args in invalid:
            check_error(client, args)
        check(client, ["HGET", key, "n"], b"9223372036854775807")
        check(client, ["HPEXPIRETIME", key, "FIELDS", 1, "n"], [absolute])
        assert 0 < client.command("PTTL", key) <= 60000
        check(client, ["HINCRBY", key, "n", -1], 9223372036854775806)
        check(client, ["HPEXPIRETIME", key, "FIELDS", 1, "n"], [absolute])
        check(client, ["HSETEX", key, "KEEPTTL", "FIELDS", 1,
                       "n", -9223372036854775808], 1)
        check_error(client, ["HINCRBY", key, "n", -1])
        check(client, ["HGET", key, "n"], b"-9223372036854775808")
        check(client, ["SET", wrong, "string"], b"OK")
        for command, extra in [("HGET", ["f"]), ("HSET", ["f", "v"]),
                               ("HLEN", []), ("HSCAN", [0]),
                               ("HPTTL", ["FIELDS", 1, "f"])]:
            check_error(client, [command, wrong, *extra])
        check(client, ["GET", wrong], b"string")

        # A field expiry deleting the final field also deletes its key TTL/type.
        check(client, ["HSETEX", last, "PX", 40, "FIELDS", 1, "f", "v"], 1)
        check(client, ["PEXPIRE", last, 60000], 1)
        wait_until_missing(client, last, "f")
        check(client, ["HLEN", last], 0)
        check(client, ["HGETALL", last], [])
        check(client, ["HPTTL", last, "FIELDS", 1, "f"], [-2])
        check(client, ["EXISTS", last], 0)
        check(client, ["TYPE", last], b"none")
        check(client, ["SET", last, "reused"], b"OK")
        check(client, ["PTTL", last], -1)
        client.command("DEL", last)
        check(client, ["HSETEX", last, "PX", 60000, "FIELDS", 1, "f", "v"], 1)
        check(client, ["PEXPIRE", last, 0], 1)
        check(client, ["HPTTL", last, "FIELDS", 1, "f"], [-2])
        print("PASS binary hashes, duplicates, atomic conditions, field/key TTLs and error nonmutation", flush=True)
    finally:
        client.command("DEL", *all_keys)


def scan_mutation(client):
    key = PREFIX + b"scan"
    client.command("DEL", key)
    stable = {f"stable:{i}".encode() for i in range(80)}
    transient = {f"transient:{i}".encode() for i in range(80)}
    try:
        args = ["HSET", key]
        for field in stable | transient:
            args += [field, b"value:" + field]
        check(client, args, len(stable | transient))
        cursor, seen, steps = b"0", set(), 0
        while True:
            cursor, values = client.command("HSCAN", key, cursor, "COUNT", 3)
            batch = pairs(values)
            for field, value in batch.items():
                assert value == b"value:" + field, (field, value)
            seen.update(batch)
            if steps == 0:
                client.command("HDEL", key, *transient)
            new = f"new:{steps}".encode()
            client.command("HSET", key, new, b"value:" + new)
            steps += 1
            assert steps < 2000, "HSCAN did not terminate"
            if cursor == b"0":
                break
        assert stable <= seen, ("continuous fields were skipped", stable - seen)
        cursor, filtered = b"0", set()
        while True:
            cursor, values = client.command("HSCAN", key, cursor, "MATCH", "stable:*", "COUNT", 2, "NOVALUES")
            filtered.update(values)
            if cursor == b"0":
                break
        assert filtered == stable, (filtered, stable)
        check(client, ["HSET", key, b"\xff\x00field", b"binary"], 1)
        cursor, seen_binary = b"0", {}
        while True:
            cursor, values = client.command("HSCAN", key, cursor, "MATCH", b"\xff?*")
            seen_binary.update(pairs(values))
            if cursor == b"0":
                break
        assert seen_binary == {b"\xff\x00field": b"binary"}, seen_binary
        print("PASS hash scan continuous-field mutation guarantee, binary MATCH and NOVALUES", flush=True)
    finally:
        client.command("DEL", key)


def crash_recovery(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-hashes-crash-") as directory:
        with Server(binary, directory, durability="strict") as server:
            client = server.connect()
            key, offline, last = PREFIX + b"durable", PREFIX + b"offline", PREFIX + b"offline-last"
            field, payload = b"\xff\x00", b"\x00\r\n\xff"
            check(client, ["HSET", key, field, payload, "keep", 1, "remove", "old"], 3)
            check(client, ["HSETEX", key, "PX", 40, "FIELDS", 1, "passive", "must-not-return"], 1)
            wait_until_missing(client, key, "passive")
            check(client, ["HINCRBY", key, "keep", 4], 5)
            check(client, ["HDEL", key, "remove", "remove", "absent"], 1)
            deadline = int(time.time() * 1000) + 60000
            check(client, ["HSETEX", key, "PXAT", deadline, "FIELDS", 1, "n", 9223372036854775807], 1)
            check(client, ["PEXPIRE", key, 60000], 1)
            check_error(client, ["HINCRBY", key, "n", 1])
            check(client, ["HSETEX", key, "FNX", "FIELDS", 2, "bad-new", "bad", "keep", "bad"], 0)
            check(client, ["HSET", offline, "stay", "durable"], 1)
            offline_deadline = int(time.time() * 1000) + 1200
            check(client, ["HSETEX", offline, "PXAT", offline_deadline, "FIELDS", 1, "gone", "offline"], 1)
            check(client, ["HSETEX", last, "PXAT", offline_deadline, "FIELDS", 1, "gone", "offline"], 1)
            server.stop(signal.SIGKILL)
            # The process remains down while these absolute field deadlines pass.
            delay = offline_deadline / 1000 - time.time() + 0.05
            assert delay > 0, "fixture expired before the offline interval began"
            time.sleep(delay)
            server.start()
            client = server.connect()
            assert pairs(client.command("HGETALL", key)) == {
                field: payload, b"keep": b"5", b"n": b"9223372036854775807",
            }
            check(client, ["HPEXPIRETIME", key, "FIELDS", 1, "n"], [deadline])
            assert 0 < client.command("PTTL", key) < 60000
            check(client, ["HGETALL", offline], [b"stay", b"durable"])
            check(client, ["EXISTS", last], 0)
            check(client, ["SET", last, "new-type"], b"OK")
            check(client, ["HINCRBY", key, "keep", 1], 6)
            check(client, ["HPERSIST", key, "FIELDS", 1, "n"], [1])
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            check(client, ["HMGET", key, "keep", "passive", "remove", "bad-new"], [b"6", None, None, None])
            check(client, ["HPTTL", key, "FIELDS", 1, "n"], [-1])
            check(client, ["GET", last], b"new-type")
            check(client, ["HSETEX", offline, "PXAT", 0, "FIELDS", 1, "stay", "gone"], 1)
            server.stop(signal.SIGKILL)
            server.start()
            check(server.connect(), ["EXISTS", offline], 0)
    print("PASS actual SIGKILL/reopen, offline field deadlines, passive-expiry nonresurrection and later writes", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference", help="Redis 8 host:port; writes only ffkv:test:hashes: keys")
    options = parser.parse_args()
    with Server(options.binary) as server:
        client = server.connect()
        exercise(client)
        scan_mutation(client)
    crash_recovery(options.binary)
    if options.reference:
        host, port = options.reference.rsplit(":", 1)
        reference = Client((host, int(port)))
        try:
            exercise(reference)
            scan_mutation(reference)
        finally:
            reference.close()
        print("PASS same hash consumer scenarios against optional Redis 8 reference", flush=True)


if __name__ == "__main__":
    main()
