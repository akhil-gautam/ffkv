#!/usr/bin/env python3
"""Sorted-set boundaries, continuous scans, crash recovery and optional Redis differential.

Usage: python3 tests/sorted_sets_smoke.py [--binary target/debug/ffkv]
       python3 tests/sorted_sets_smoke.py --reference 127.0.0.1:16380 --seed 4217
Only owned ffkv:test:sorted-sets:* keys are changed on the optional reference.
Score text is compared numerically; member bytes are never normalized.
"""
import argparse
import random
import signal
import tempfile

from cli_smoke import Client
from durability_smoke import Server


PREFIX = b"ffkv:test:sorted-sets:"


def is_error(value):
    return isinstance(value, tuple) and len(value) == 2 and value[0] == "error"


def numeric(value):
    return None if value is None else float(value)


def normalize(args, value):
    """Normalize only response positions that the command declares as scores."""
    if is_error(value):
        return ("error", value[1].split(b" ", 1)[0])
    command = str(args[0]).upper() if not isinstance(args[0], bytes) else args[0].decode().upper()
    options = [arg.upper() for arg in args[2:] if isinstance(arg, (str, bytes))]
    has = lambda name: name in options or name.encode() in options
    if command in ("ZSCORE", "ZINCRBY") or command == "ZADD" and has("INCR"):
        return numeric(value)
    if command == "ZMSCORE":
        return [numeric(item) for item in value]
    if command in ("ZRANK", "ZREVRANK") and has("WITHSCORE"):
        return None if value is None else [value[0], numeric(value[1])]
    if command in ("ZPOPMIN", "ZPOPMAX") or command in (
        "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE",
    ) and has("WITHSCORES"):
        return [numeric(item) if index % 2 else item for index, item in enumerate(value)]
    if command == "ZSCAN":
        return [value[0], [numeric(item) if index % 2 else item for index, item in enumerate(value[1])]]
    return value


def check(client, args, expected):
    actual = client.command(*args)
    assert normalize(args, actual) == normalize(args, expected), (args, actual, expected)
    return actual


def check_error(client, args):
    actual = client.command(*args)
    assert is_error(actual), (args, actual)


def snapshot(client, key):
    return normalize(["ZRANGE", key, 0, -1, "WITHSCORES"], client.command("ZRANGE", key, 0, -1, "WITHSCORES"))


def boundaries(client):
    key = PREFIX + b"boundaries"
    client.command("DEL", key)
    try:
        check(client, ["ZADD", key, "-inf", b"low", "inf", b"high", -0.0, b"", 0, b"\x00", 0, b"01", 0, b"1", 0, b"\xff", 1, b"one"], 8)
        ordered = [b"low", b"", b"\x00", b"01", b"1", b"\xff", b"one", b"high"]
        check(client, ["ZRANGE", key, 0, -1], ordered)
        check(client, ["ZREVRANGE", key, 0, -1], ordered[::-1])
        check(client, ["ZRANGE", key, 0, -1, "REV"], ordered[::-1])
        check(client, ["ZSCORE", key, b""], b"0")
        check(client, ["ZMSCORE", key, b"01", "missing", "high"], [b"0", None, b"inf"])
        check(client, ["ZCARD", key], 8)
        for rank, member in enumerate(ordered):
            check(client, ["ZRANK", key, member], rank)
            check(client, ["ZREVRANK", key, member], 7 - rank)
        check(client, ["ZRANK", key, b"01", "WITHSCORE"], [3, b"0"])
        check(client, ["ZREVRANK", key, "absent", "WITHSCORE"], None)
        check(client, ["ZRANGE", key, -(2**63), 2**63 - 1], ordered)
        check(client, ["ZRANGE", key, 2**63 - 1, -1], [])
        check(client, ["ZRANGE", key, -(2**63), -(2**63)], [])
        check(client, ["ZRANGE", key, -3, -2], ordered[-3:-1])
        check(client, ["ZCOUNT", key, 0, 0], 5)
        check(client, ["ZCOUNT", key, "(0", "(inf"], 1)
        check(client, ["ZCOUNT", key, "-inf", "-inf"], 1)
        check(client, ["ZCOUNT", key, "(inf", "inf"], 0)
        check(client, ["ZCOUNT", key, "-inf", "(-inf"], 0)
        check(client, ["ZRANGE", key, 0, 0, "BYSCORE"], ordered[1:6])
        check(client, ["ZRANGEBYSCORE", key, "(-inf", "(inf"], ordered[1:-1])
        check(client, ["ZREVRANGEBYSCORE", key, 0, 0], ordered[1:6][::-1])
        check(client, ["ZRANGE", key, "inf", "-inf", "BYSCORE", "REV", "LIMIT", 2, 3], ordered[::-1][2:5])
        check(client, ["ZRANGEBYSCORE", key, "-inf", "inf", "LIMIT", 2, -99], ordered[2:])
        check(client, ["ZRANGEBYSCORE", key, "-inf", "inf", "LIMIT", -1, 2], [])
        check(client, ["ZRANGEBYSCORE", key, "-inf", "inf", "LIMIT", 0, 0], [])
        check(client, ["ZRANGE", key, "inf", "-inf", "BYSCORE"], [])
        for args in (["ZRANGE", key, 0, -1, "LIMIT", 0, 1], ["ZRANGE", key, 0, -1, "WITHSCORE"],
                     ["ZRANK", key, "01", "WITHSCORES"], ["ZCOUNT", key, "(nan", "inf"],
                     ["ZPOPMIN", key, -1], ["ZPOPMAX", key, 2**63]):
            check_error(client, args)
        assert snapshot(client, key)[::2] == ordered
        check(client, ["ZPOPMAX", key, 3], [b"high", b"inf", b"one", b"1", b"\xff", b"0"])
        check(client, ["ZPOPMIN", key, 0], [])
        check(client, ["ZPOPMIN", key, 99], [b"low", b"-inf", b"", b"0", b"\x00", b"0", b"01", b"0", b"1", b"0"])
        check(client, ["EXISTS", key], 0)
        check(client, ["ZPOPMAX", key], [])
        check(client, ["ZSCAN", key, 0], [b"0", []])
        check(client, ["ZADD", key, -1, "a", 0, "b", 1, "c", 2, "d"], 4)
        check(client, ["ZREMRANGEBYRANK", key, -3, -2], 2)
        check(client, ["ZRANGE", key, 0, -1], [b"a", b"d"])
        check(client, ["ZREMRANGEBYSCORE", key, "(-1", "+inf"], 1)
        check(client, ["ZREMRANGEBYRANK", key, -(2**63), 2**63 - 1], 1)
        check(client, ["EXISTS", key], 0)
        check(client, ["ZADD", key, "-inf", "negative-infinity", "-1.7976931348623157e308", "negative-max",
                       "-5e-324", "negative-tiny", "-0", "zero", "5e-324", "tiny",
                       "1.7976931348623157e308", "max", "inf", "infinity"], 7)
        check(client, ["ZRANGEBYSCORE", key, "(-inf", "(-5e-324"], [b"negative-max"])
        check(client, ["ZRANGEBYSCORE", key, "(-5e-324", "(5e-324"], [b"zero"])
        check(client, ["ZRANGEBYSCORE", key, "(1.7976931348623157e308", "inf"], [b"infinity"])
        check(client, ["ZREVRANGEBYSCORE", key, "5e-324", "-5e-324"], [b"tiny", b"zero", b"negative-tiny"])
    finally:
        client.command("DEL", key)


def updates(client):
    key = PREFIX + b"updates"
    client.command("DEL", key)
    try:
        check(client, ["ZADD", key, "XX", 1, "a"], 0)
        check(client, ["EXISTS", key], 0)
        check(client, ["ZADD", key, "CH", 1, "a", 2, "a", 3, "a"], 3)
        check(client, ["ZADD", key, 4, "a", 5, "a", 1, "b", 2, "b"], 1)
        check(client, ["ZADD", key, "NX", "CH", 9, "a", 1, "c", 2, "c"], 1)
        check(client, ["ZADD", key, "XX", "CH", 6, "a", 6, "a", 9, "absent"], 1)
        check(client, ["ZADD", key, "GT", "CH", 5, "a", 7, "a", -5, "new"], 2)
        check(client, ["ZADD", key, "LT", "CH", 8, "a", 3, "a", 0, "newer"], 2)
        check(client, ["ZADD", key, "XX", "GT", "CH", 10, "absent"], 0)
        check(client, ["ZADD", key, "INCR", "GT", -1, "a"], None)
        check(client, ["ZADD", key, "INCR", "LT", -1, "a"], b"2")
        check(client, ["ZADD", key, "INCR", "XX", 1, "absent"], None)
        check(client, ["ZADD", key, "INCR", "NX", "-inf", "a"], None)
        check(client, ["ZINCRBY", key, 0, "a"], b"2")
        check(client, ["ZINCRBY", key, 0.5, "a"], b"2.5")
        check(client, ["ZADD", key, "inf", "infinite"], 1)
        for args in (["ZADD", key, "NX", "XX", 1, "x"], ["ZADD", key, "NX", "GT", 1, "x"],
                     ["ZADD", key, "NX", "LT", 1, "x"], ["ZADD", key, "GT", "LT", 1, "x"],
                     ["ZADD", key, "INCR", 1, "a", 2, "b"], ["ZADD", key, 1, "x", "nan", "y"],
                     ["ZADD", key, 1, "a", "1e999", "y"], ["ZADD", key, "1e-999", "y"],
                     ["ZINCRBY", key, "-inf", "infinite"], ["ZADD", key, "INCR", "LT", "-inf", "infinite"],
                     ["ZINCRBY", key, "nan", "absent"], ["ZADD", key, 1, "x", 2]):
            before = snapshot(client, key)
            check_error(client, args)
            assert snapshot(client, key) == before, args
        # Arithmetic overflow is a valid infinity; an undefined inf + -inf is not.
        check(client, ["ZADD", key, "1.7976931348623157e308", "overflow"], 1)
        check(client, ["ZINCRBY", key, "1.7976931348623157e308", "overflow"], b"inf")
        check(client, ["ZREM", key, "a", "a", "absent"], 1)
        check(client, ["PEXPIRE", key, 60000], 1)
        ttl = client.command("PTTL", key)
        check(client, ["ZADD", key, "CH", 12, "b"], 1)
        check(client, ["ZINCRBY", key, 1, "b"], b"13")
        check(client, ["ZREM", key, "c"], 1)
        assert 0 < client.command("PTTL", key) <= ttl
        check(client, ["PEXPIREAT", key, 1], 1)
        check(client, ["ZCARD", key], 0)
        check(client, ["ZADD", key, 4, "fresh"], 1)
        check(client, ["PTTL", key], -1)
        check(client, ["ZSCAN", key, 0], [b"0", [b"fresh", b"4"]])
        check(client, ["ZREM", key, "fresh"], 1)
        check(client, ["TYPE", key], b"none")
        client.command("SET", key, "string")
        for args in (["ZCARD", key], ["ZMSCORE", key, "x"], ["ZADD", key, 1, "x"],
                     ["ZPOPMIN", key, 0], ["ZRANGE", key, 0, -1], ["ZSCAN", key, 0],
                     ["ZREMRANGEBYSCORE", key, 10, -10]):
            check_error(client, args)
        check(client, ["GET", key], b"string")
    finally:
        client.command("DEL", key)


def scans(client):
    key = PREFIX + b"scan"
    client.command("DEL", key)
    survivors = {b"member:%04d" % index for index in range(180)} | {b"01", b"1", b"\x00\xff"}
    try:
        args = ["ZADD", key]
        for index, member in enumerate(sorted(survivors)):
            args.extend([index, member])
        check(client, args, len(survivors))
        seen = set()
        cursor = 0
        iterations = 0
        while True:
            cursor, pairs = client.command("ZSCAN", key, cursor, "COUNT", 1)
            assert len(pairs) % 2 == 0
            seen.update(pairs[::2])
            # Never remove a survivor. Stable hash cursors must still visit all
            # continuously present members despite changing scores and churn.
            member = sorted(survivors)[iterations % len(survivors)]
            client.command("ZINCRBY", key, 1, member)
            transient = b"transient:%04d" % iterations
            client.command("ZADD", key, iterations, transient)
            if iterations:
                client.command("ZREM", key, b"transient:%04d" % (iterations - 1))
            iterations += 1
            assert iterations < 10000, "ZSCAN cursor failed to terminate"
            if cursor == b"0":
                break
        assert survivors <= seen, survivors - seen
        cursor, matched = 0, set()
        while True:
            cursor, pairs = client.command("ZSCAN", key, cursor, "MATCH", b"member:00[0-9][0-9]", "COUNT", 3)
            matched.update(pairs[::2])
            if cursor == b"0":
                break
        assert matched == {b"member:%04d" % index for index in range(100)}
        for args in (["ZSCAN", key, -1], ["ZSCAN", key, 0, "COUNT", 0], ["ZSCAN", key, 0, "TYPE", "zset"]):
            check_error(client, args)
        check(client, ["ZREMRANGEBYRANK", key, 0, -1], client.command("ZCARD", key))
        check(client, ["ZSCAN", key, 0], [b"0", []])
    finally:
        client.command("DEL", key)


def persistence(binary):
    key = PREFIX + b"persistent"
    with tempfile.TemporaryDirectory(prefix="ffkv-sorted-sets-") as directory:
        with Server(binary, directory, durability="strict") as server:
            client = server.connect()
            check(client, ["ZADD", key, 1, "a", 2, "b", 2, "c", 3, "d", 4, "e", 5, "f"], 6)
            check(client, ["PEXPIRE", key, 600000], 1)
            stages = [
                (["ZADD", key, "GT", "CH", 0, "a", 7, "b", 6, "b", 8, "new"], 2),
                (["ZADD", key, "XX", "LT", "INCR", -2, "b"], b"5"),
                (["ZREMRANGEBYSCORE", key, "(2", 4], 2),
                (["ZREMRANGEBYRANK", key, 0, 0], 1),
                (["ZPOPMAX", key, 2], [b"new", b"8", b"f", b"5"]),
                (["ZPOPMIN", key, 10], [b"c", b"2", b"b", b"5"]),
                (["ZADD", key, -0.0, b"\x00", "inf", b"\xff"], 2),
            ]
            for args, expected in stages:
                check(client, args, expected)
                before = snapshot(client, key)
                before_ttl = client.command("PTTL", key)
                server.stop(signal.SIGKILL)
                server.start()
                client = server.connect()
                assert snapshot(client, key) == before, args
                after_ttl = client.command("PTTL", key)
                if before_ttl > 0:
                    assert 0 < after_ttl <= before_ttl, (args, before_ttl, after_ttl)
                else:
                    assert after_ttl == before_ttl
                # Recovery must restore both the score order and member scan index.
                cursor, observed = 0, {}
                while True:
                    cursor, pairs = client.command("ZSCAN", key, cursor, "COUNT", 1)
                    observed.update(zip(pairs[::2], map(float, pairs[1::2])))
                    if cursor == b"0":
                        break
                assert observed == dict(zip(before[::2], before[1::2]))
                for rank, member in enumerate(before[::2]):
                    check(client, ["ZRANK", key, member], rank)


def differential(client, reference, seed):
    other = Client(reference)
    key = PREFIX + b"differential"
    rng = random.Random(seed)
    members = [b"", b"0", b"00", b"1", b"01", b"10", b"-0", b"\x00\xff", b"z", b"\xff"]
    scores = [-4, -1, "-0", 0, 0.25, 1, 2, 9, "inf", "-inf"]
    bounds = ["-inf", "inf", "(-inf", "(inf", -1, "(-1", 0, "(0", 2, "(2", 9]
    ranks = [-(2**63), -20, -3, -1, 0, 1, 4, 20, 2**63 - 1]
    try:
        client.command("DEL", key)
        other.command("DEL", key)
        for step in range(700):
            choice = rng.randrange(17)
            member = rng.choice(members)
            if choice < 3:
                flags = rng.choice([[], ["NX"], ["XX"], ["GT"], ["LT"], ["XX", "GT"], ["XX", "LT"], ["NX", "XX"], ["NX", "GT"]])
                if rng.randrange(2):
                    flags += ["CH"]
                if rng.randrange(4) == 0:
                    flags += ["INCR"]
                    pairs = [rng.choice(scores), member]
                else:
                    pairs = []
                    for _ in range(rng.randint(1, 4)):
                        pairs += [rng.choice(scores), rng.choice(members)]
                args = ["ZADD", key] + flags + pairs
            elif choice == 3:
                args = ["ZINCRBY", key, rng.choice(scores), member]
            elif choice == 4:
                args = ["ZREM", key] + rng.choices(members, k=rng.randint(1, 4))
            elif choice == 5:
                args = ["ZMSCORE", key] + rng.choices(members, k=4)
            elif choice == 6:
                args = [rng.choice(["ZRANK", "ZREVRANK"]), key, member] + rng.choice([[], ["WITHSCORE"]])
            elif choice == 7:
                args = ["ZCOUNT", key, rng.choice(bounds), rng.choice(bounds)]
            elif choice == 8:
                args = [rng.choice(["ZRANGE", "ZREVRANGE"]), key, rng.choice(ranks), rng.choice(ranks)]
                args += rng.choice([[], ["WITHSCORES"]])
            elif choice in (9, 10):
                args = [rng.choice(["ZRANGEBYSCORE", "ZREVRANGEBYSCORE"]), key, rng.choice(bounds), rng.choice(bounds)]
                args += rng.choice([[], ["LIMIT", rng.choice([-1, 0, 1, 8]), rng.choice([-9, 0, 1, 3])]])
                args += rng.choice([[], ["WITHSCORES"]])
            elif choice == 11:
                args = ["ZRANGE", key, rng.choice(bounds), rng.choice(bounds), "BYSCORE"]
                args += rng.choice([[], ["REV"]]) + ["LIMIT", rng.choice([-1, 0, 1, 5]), rng.choice([-1, 0, 2])]
                args += rng.choice([[], ["WITHSCORES"]])
            elif choice == 12:
                args = ["ZREMRANGEBYRANK", key, rng.choice(ranks), rng.choice(ranks)]
            elif choice == 13:
                args = ["ZREMRANGEBYSCORE", key, rng.choice(bounds), rng.choice(bounds)]
            elif choice == 14:
                args = [rng.choice(["ZPOPMIN", "ZPOPMAX"]), key] + rng.choice([[], [0], [1], [3], [-1]])
            elif choice == 15:
                args = ["ZSCORE", key, member]
            else:
                args = ["ZCARD", key]
            actual, expected = client.command(*args), other.command(*args)
            assert normalize(args, actual) == normalize(args, expected), (seed, step, args, actual, expected)
            assert snapshot(client, key) == snapshot(other, key), (seed, step, args)
        # Redis and ffkv use different scan indexes: compare complete scans, not cursors.
        def full_scan(connection):
            cursor, result = 0, {}
            while True:
                cursor, pairs = connection.command("ZSCAN", key, cursor, "COUNT", 1)
                result.update(zip(pairs[::2], map(float, pairs[1::2])))
                if cursor == b"0":
                    return result
        assert full_scan(client) == full_scan(other)
    finally:
        client.command("DEL", key)
        other.command("DEL", key)
        other.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference")
    parser.add_argument("--seed", type=int, default=4217)
    args = parser.parse_args()
    with Server(args.binary) as server:
        client = server.connect()
        boundaries(client)
        updates(client)
        scans(client)
        if args.reference:
            host, port = args.reference.rsplit(":", 1)
            differential(client, (host, int(port)), args.seed)
    persistence(args.binary)
    print("PASS sorted-set boundaries, conditions, scans, expiry, and SIGKILL recovery")
    if args.reference:
        print(f"PASS seeded Redis sorted-set differential (seed={args.seed})")


if __name__ == "__main__":
    main()
