#!/usr/bin/env python3
"""Exercise set commands through real sockets and SIGKILL/WAL recovery.

Usage: python3 tests/sets_smoke.py [--binary target/debug/ffkv]
       python3 tests/sets_smoke.py --reference 127.0.0.1:16380
Only keys prefixed ffkv:test:sets: are touched on the optional reference.
Unordered replies and error categories, not random choices/error prose, compare.
"""
import argparse
from pathlib import Path
import random
import signal
import tempfile

from cli_smoke import Client
from durability_smoke import Server


PREFIX = b"ffkv:test:sets:"
UNORDERED = {"SMEMBERS", "SUNION", "SINTER", "SDIFF"}


def is_error(value):
    return isinstance(value, tuple) and len(value) == 2 and value[0] == "error"


def normalized(value):
    if is_error(value):
        return "error", value[1].split(b" ", 1)[0]
    return value


def check(client, args, expected):
    actual = client.command(*args)
    assert actual == expected, (args, actual, expected)
    return actual


def check_error(client, args, category=b"ERR"):
    actual = client.command(*args)
    assert normalized(actual) == ("error", category), (args, actual, category)


def member_set(client, key):
    actual = client.command("SMEMBERS", key)
    assert isinstance(actual, list) and all(isinstance(x, bytes) for x in actual), actual
    assert len(actual) == len(set(actual)), ("duplicate member", actual)
    return set(actual)


def snapshot(client, keys):
    result = {}
    for key in keys:
        kind = client.command("TYPE", key)
        if kind == b"none":
            result[key] = (kind, None)
        elif kind == b"set":
            result[key] = (kind, frozenset(member_set(client, key)))
        elif kind == b"string":
            result[key] = (kind, client.command("GET", key))
        else:
            raise AssertionError(("unexpected key type", key, kind))
    return result


def scan_all(client, key, match=None, count=3):
    cursor = b"0"
    found = set()
    for _ in range(10000):
        args = ["SSCAN", key, cursor, "COUNT", count]
        if match is not None:
            args += ["MATCH", match]
        reply = client.command(*args)
        assert isinstance(reply, list) and len(reply) == 2, reply
        cursor, values = reply
        assert isinstance(cursor, bytes) and isinstance(values, list), reply
        found.update(values)
        if cursor == b"0":
            return found
    raise AssertionError("SSCAN did not terminate")


def deterministic(client):
    a, b, bad, missing = [PREFIX + name for name in (b"a", b"b", b"bad", b"missing")]
    keys = [a, b, bad, missing]
    client.command("DEL", *keys)
    try:
        for command in ("SMEMBERS", "SUNION", "SINTER", "SDIFF"):
            check(client, [command, missing], [])
        for command in ("SPOP", "SRANDMEMBER"):
            check(client, [command, missing], None)
            for count in (0, 1, 9):
                check(client, [command, missing, count], [])
        check(client, ["SRANDMEMBER", missing, -3], [])
        check(client, ["SCARD", missing], 0)
        check(client, ["SISMEMBER", missing, b"x"], 0)
        check(client, ["SMISMEMBER", missing, b"x", b"x"], [0, 0])
        check(client, ["SADD", a, b"a", b"a", b"b", b"c", b"b", b"\x00\xff", b""], 5)
        check(client, ["SADD", a, b"b", b"a"], 0)
        check(client, ["SMISMEMBER", a, b"a", b"missing", b"a", b""], [1, 0, 1, 1])
        check(client, ["SREM", a, b"a", b"a", b"missing"], 1)
        assert member_set(client, a) == {b"b", b"c", b"\x00\xff", b""}
        check(client, ["SREM", a, b"", b"\x00\xff"], 2)
        assert scan_all(client, a) == {b"b", b"c"}
        assert scan_all(client, a, match=b"b*") == {b"b"}
        check(client, ["SADD", b, b"c", b"d"], 2)
        for command, expected in (("SUNION", {b"b", b"c", b"d"}), ("SINTER", {b"c"}), ("SDIFF", {b"b"})):
            assert set(client.command(command, a, b)) == expected
        assert set(client.command("SUNION", a, a, b)) == {b"b", b"c", b"d"}
        assert set(client.command("SINTER", a, a, b)) == {b"c"}
        check(client, ["SDIFF", a, a], [])
        check(client, ["SMOVE", a, a, b"b"], 1)
        check(client, ["SMOVE", a, a, b"missing"], 0)
        check(client, ["SMOVE", a, b, b"c"], 1)
        assert member_set(client, a) == {b"b"}
        assert member_set(client, b) == {b"c", b"d"}
        check(client, ["SMOVE", a, b, b"b"], 1)
        check(client, ["TYPE", a], b"none")
        check(client, ["SMOVE", a, b, b"b"], 0)
        check(client, ["SADD", a, b"b", b"e"], 2)
        check(client, ["PEXPIRE", a, 60000], 1)
        check(client, ["PEXPIRE", b, 60000], 1)
        check(client, ["SADD", a, b"temporary"], 1)
        check(client, ["SREM", a, b"temporary"], 1)
        check(client, ["SMOVE", a, b, b"e"], 1)
        assert 0 < client.command("PTTL", a) <= 60000
        assert 0 < client.command("PTTL", b) <= 60000
        check(client, ["SUNIONSTORE", a, a, b], 4)
        assert member_set(client, a) == {b"b", b"c", b"d", b"e"}
        check(client, ["PTTL", a], -1)
        check(client, ["SDIFFSTORE", a, a, b], 0)
        check(client, ["TYPE", a], b"none")
        check(client, ["SINTERSTORE", b, b, missing], 0)
        check(client, ["TYPE", b], b"none")
        check(client, ["SET", bad, b"unchanged"], b"OK")
        check(client, ["SADD", a, b"survivor"], 1)
        before = snapshot(client, keys)
        invalid = [
            ["SADD", bad, b"x"], ["SREM", bad, b"x"], ["SMEMBERS", bad],
            ["SCARD", bad], ["SISMEMBER", bad, b"x"], ["SMISMEMBER", bad, b"x", b"x"],
            ["SPOP", bad, 0], ["SRANDMEMBER", bad, 0], ["SSCAN", bad, 0],
            ["SMOVE", a, bad, b"survivor"], ["SMOVE", a, bad, b"absent"],
            ["SMOVE", bad, a, b"x"],
        ]
        for operation in ("SUNION", "SINTER", "SDIFF"):
            invalid += [[operation, missing, bad], [operation + "STORE", a, missing, bad]]
        for args in invalid:
            check_error(client, args, b"WRONGTYPE")
            assert snapshot(client, keys) == before, args
        check(client, ["SMOVE", missing, bad, b"x"], 0)
        for args in (["SPOP", a, -1], ["SPOP", a, "-0"], ["SPOP", a, "01"],
                     ["SRANDMEMBER", a, "-9223372036854775808"],
                     ["SSCAN", a, "bad"], ["SSCAN", a, 0, "COUNT", 0],
                     ["SSCAN", a, 0, "TYPE", "set"], ["SSCAN", a, 0, "MATCH"],
                     ["SADD", a], ["SPOP", a, 1, 2], ["SRANDMEMBER", a, 1, 2]):
            check_error(client, args)
            assert snapshot(client, keys) == before, args
        # Destination types are replaceable when not also an invalid source.
        check(client, ["SUNIONSTORE", bad, a], 1)
        assert member_set(client, bad) == {b"survivor"}
        check(client, ["PTTL", bad], -1)
        check(client, ["SREM", a, b"survivor", b"survivor"], 1)
        check(client, ["TYPE", a], b"none")
    finally:
        client.command("DEL", *keys)
    print("PASS duplicate membership, binary values, swap removals, algebra overlap, TTL and nonmutating errors", flush=True)


def random_selection(client):
    key = PREFIX + b"selection"
    client.command("DEL", key)
    try:
        all_members = {b"member:%d" % index for index in range(100)}
        check(client, ["SADD", key, *sorted(all_members)], 100)
        check(client, ["SPOP", key, 0], [])
        check(client, ["SRANDMEMBER", key, 0], [])
        assert client.command("SRANDMEMBER", key) in all_members
        for count in (1, 17, 99, 100, 200, 9223372036854775807):
            values = client.command("SRANDMEMBER", key, count)
            assert len(values) == min(count, 100)
            assert len(set(values)) == len(values) and set(values) <= all_members
        for count in (-1, -100, -201):
            values = client.command("SRANDMEMBER", key, count)
            assert len(values) == -count and set(values) <= all_members
        assert member_set(client, key) == all_members
        check(client, ["PEXPIRE", key, 60000], 1)
        popped = client.command("SPOP", key, 87)
        assert len(popped) == 87 and len(set(popped)) == 87 and set(popped) <= all_members
        remaining = member_set(client, key)
        assert len(remaining) == 13 and not remaining.intersection(popped)
        assert remaining.union(popped) == all_members
        assert 0 < client.command("PTTL", key) <= 60000
        one = client.command("SPOP", key)
        assert one in remaining
        remaining.remove(one)
        popped = client.command("SPOP", key, 9223372036854775807)
        assert len(popped) == 12 and set(popped) == remaining
        check(client, ["TYPE", key], b"none")
        check(client, ["SADD", key, b"only"], 1)
        assert client.command("SRANDMEMBER", key, -7) == [b"only"] * 7
        check(client, ["SPOP", key], b"only")
        check(client, ["SPOP", key], None)
    finally:
        client.command("DEL", key)
    print("PASS random sample shape, no replacement, duplicate sampling, pop remaining membership and deletion", flush=True)


def mutating_scan(client):
    key = PREFIX + b"scan"
    stable = {b"stable:%d" % index for index in range(150)}
    transient = {b"transient:%d" % index for index in range(150)}
    client.command("DEL", key)
    client.command("SADD", key, *sorted(stable | transient))
    cursor, found = b"0", set()
    try:
        for iteration in range(10000):
            cursor, values = client.command("SSCAN", key, cursor, "COUNT", 2)
            found.update(values)
            client.command("SREM", key, *sorted(transient))
            client.command("SADD", key, b"new:%d" % iteration)
            if cursor == b"0":
                break
        else:
            raise AssertionError("SSCAN mutation loop did not terminate")
        assert stable <= found, ("continuously present members were skipped", stable - found)
        assert scan_all(client, key, match=b"stable:*") == stable
    finally:
        client.command("DEL", key)
    print("PASS hash-cursor SSCAN retains continuously present members across mutations", flush=True)


def validate_random_reply(reply, before, count):
    if count is None:
        assert reply is None if not before else reply in before, (reply, before, count)
        return set() if reply is None else {reply}
    assert isinstance(reply, list), (reply, count)
    assert len(reply) == (abs(count) if count < 0 and before else min(abs(count), len(before))), (reply, before, count)
    assert set(reply) <= before, (reply, before, count)
    if count >= 0:
        assert len(reply) == len(set(reply)), ("duplicate without replacement", reply)
    return set(reply)


def differential(client, reference):
    other = Client(reference)
    rng = random.Random(48291)
    keys = [PREFIX + b"random:%d" % index for index in range(8)]
    values = [b"", b"a", b"b", b"c", b"\x00\xff", b"1", b"two"]
    try:
        for connection in (client, other):
            connection.command("DEL", *keys)
        for iteration in range(1500):
            key = rng.choice(keys)
            source_keys = [rng.choice(keys) for _ in range(rng.randint(1, 4))]
            arguments = [rng.choice(values) for _ in range(rng.randint(1, 5))]
            operation = rng.randrange(21)
            args = [
                ["SADD", key, *arguments], ["SREM", key, *arguments],
                ["SMEMBERS", key], ["SCARD", key], ["SISMEMBER", key, arguments[0]],
                ["SMISMEMBER", key, *arguments], ["SMOVE", key, rng.choice(keys), arguments[0]],
                ["SUNION", *source_keys], ["SINTER", *source_keys], ["SDIFF", *source_keys],
                ["SUNIONSTORE", key, *source_keys], ["SINTERSTORE", key, *source_keys],
                ["SDIFFSTORE", key, *source_keys], ["DEL", key, key], ["SET", key, arguments[0]],
                ["TYPE", key], ["SRANDMEMBER", key], ["SRANDMEMBER", key, rng.randint(-12, 12)],
                ["SPOP", key], ["SPOP", key, rng.randint(0, 12)],
                ["SRANDMEMBER", key, rng.choice(["01", "bad", "-9223372036854775808"])],
            ][operation]
            command = args[0]
            before = snapshot(client, [key])[key]
            actual, expected = client.command(*args), other.command(*args)
            if command in ("SPOP", "SRANDMEMBER") and not is_error(actual) and not is_error(expected):
                original = set() if before[0] == b"none" else set(before[1])
                count = None if len(args) == 2 else int(args[2])
                selected = validate_random_reply(actual, original, count)
                validate_random_reply(expected, original, count)
                if command == "SPOP":
                    after = original - selected
                    assert member_set(client, key) == after
                    # Redis selects independently. Restore its contents to the
                    # actual chosen poststate before comparing later operations.
                    other.command("DEL", key)
                    if after:
                        other.command("SADD", key, *sorted(after))
                else:
                    assert member_set(client, key) == original
            else:
                if command in UNORDERED and not is_error(actual) and not is_error(expected):
                    actual, expected = sorted(actual), sorted(expected)
                assert normalized(actual) == normalized(expected), (iteration, args, actual, expected)
            assert snapshot(client, keys) == snapshot(other, keys), (iteration, args)
        # Cursor layouts may differ, but full scans and binary MATCH must agree.
        for key in keys:
            if client.command("TYPE", key) == b"set":
                assert scan_all(client, key) == scan_all(other, key)
                assert scan_all(client, key, match=b"[ab]*") == scan_all(other, key, match=b"[ab]*")
    finally:
        client.command("DEL", *keys)
        try:
            other.command("DEL", *keys)
        finally:
            other.close()
    print("PASS 1500 seeded set differential operations, unordered/error normalization and random-result invariants", flush=True)


def persistent(binary):
    keys = [PREFIX + name for name in (b"durable:source", b"durable:destination", b"durable:store")]
    source, destination, store = keys
    with tempfile.TemporaryDirectory(prefix="ffkv-sets-") as directory:
        with Server(binary, Path(directory), "strict") as server:
            client = server.connect()
            initial = {b"member:%d" % index for index in range(80)}
            check(client, ["SADD", source, *sorted(initial)], 80)
            check(client, ["SADD", destination, b"existing"], 1)
            selected = client.command("SPOP", source, 37)
            assert len(selected) == 37 and len(set(selected)) == 37
            remaining = initial - set(selected)
            assert member_set(client, source) == remaining
            saved = snapshot(client, keys)
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            assert snapshot(client, keys) == saved
            assert set(selected).isdisjoint(member_set(client, source))
            # A selected move must replay both keys as one command batch.
            moved = min(remaining)
            check(client, ["SMOVE", source, destination, moved], 1)
            saved = snapshot(client, keys)
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            assert snapshot(client, keys) == saved
            check(client, ["SISMEMBER", source, moved], 0)
            check(client, ["SISMEMBER", destination, moved], 1)
            # Replace an unrelated type, then overlap the source/destination.
            check(client, ["SET", store, b"replace-me"], b"OK")
            check(client, ["SUNIONSTORE", store, source, destination], 44)
            check(client, ["PEXPIRE", source, 60000], 1)
            before_difference = member_set(client, source) - member_set(client, destination)
            check(client, ["SDIFFSTORE", source, source, destination], len(before_difference))
            check(client, ["PTTL", source], -1)
            saved = snapshot(client, keys)
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            assert snapshot(client, keys) == saved
            assert member_set(client, source) == before_difference
            for key in keys:
                assert set(selected).isdisjoint(member_set(client, key))
            check(client, ["SINTERSTORE", destination, source, destination], 0)
            saved = snapshot(client, keys)
            server.stop(signal.SIGKILL)
            server.start()
            client = server.connect()
            assert snapshot(client, keys) == saved
            check(client, ["TYPE", destination], b"none")
    print("PASS SIGKILL/reopen preserves exact SPOP choices, atomic SMOVE and overlapping/empty STORE results", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference", metavar="HOST:PORT")
    args = parser.parse_args()
    reference = None
    if args.reference:
        host, port = args.reference.rsplit(":", 1)
        reference = host, int(port)
    with Server(args.binary) as server:
        client = server.connect()
        deterministic(client)
        random_selection(client)
        mutating_scan(client)
        if reference:
            differential(client, reference)
    persistent(args.binary)
    print("PASS set CLI and durability smoke complete", flush=True)


if __name__ == "__main__":
    main()
