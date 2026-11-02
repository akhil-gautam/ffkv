#!/usr/bin/env python3
"""Exercise real ffkv lists and blocking pops using only the Python standard library.

Usage: python3 tests/queue_smoke.py [--binary target/debug/ffkv]
       python3 tests/queue_smoke.py --reference 127.0.0.1:16380

A reference should be a disposable Redis 7+ server. Both endpoints receive unique
ffkv:test:queue:* keys, which are deleted even when an assertion fails. Reference
mode repeats the blocking scenarios and compares 1600 seeded list/string commands
plus their resulting state; it does not compare wall-clock TTLs or timings.
"""
import argparse
import contextlib
import os
import random
import select
import signal
import socket
import subprocess
import sys
import time
import uuid

from cli_smoke import Client


IO_TIMEOUT = 5
QUIET_INTERVAL = 0.05


def check(client, args, expected):
    actual = client.command(*args)
    assert actual == expected, (args, actual, expected)
    return actual


def check_error(client, args, prefix=b"ERR"):
    actual = client.command(*args)
    assert (
        isinstance(actual, tuple)
        and len(actual) == 2
        and actual[0] == "error"
        and actual[1].startswith(prefix)
    ), (args, actual, prefix)
    return actual


def expect_quiet(client, interval=QUIET_INTERVAL):
    readable, _, _ = select.select([client.socket], [], [], interval)
    assert not readable, "blocked connection replied or closed before a wakeup"


def begin_wait(client, args, control):
    client.socket.sendall(Client.frame(args))
    # There is no waiter-introspection command. A PING on this same connection
    # cannot acknowledge registration: it must remain behind the blocking pop.
    # A bounded quiet interval gives the local connection task time to enqueue
    # its request, and a different connection proves the actor keeps progressing.
    # FIFO setup below necessarily uses this scheduling allowance, not a claimed
    # cross-connection protocol barrier.
    expect_quiet(client)
    check(control, ["PING"], b"PONG")


def exact_timeout(client, args):
    started = time.monotonic()
    client.socket.sendall(Client.frame(args))
    # Client.read intentionally maps both RESP null kinds to None. Read the wire
    # header itself so a null bulk string cannot masquerade as a null array.
    actual = client.file.readline()
    assert actual == b"*-1\r\n", (args, actual, b"*-1\r\n")
    assert time.monotonic() - started < IO_TIMEOUT, (args, "timeout was not finite")
    check(client, ["PING"], b"PONG")


def cleanup_keys(address, keys):
    # Preserve the original assertion if the server itself has died. On a normal
    # run a cleanup failure is fatal rather than silently leaving reference data.
    failing = sys.exc_info()[0] is not None
    try:
        with contextlib.closing(Client(address)) as client:
            result = client.command("DEL", *keys)
            assert isinstance(result, int), ("cleanup DEL", result)
    except Exception as error:
        if not failing:
            raise
        print(f"Cleanup also failed for {address!r}: {error!r}", file=sys.stderr)


def exercise(address, prefix, label):
    names = (
        "binary\x00\xff\r\n", "count", "typed", "string", "missing", "ttl",
        "first", "second", "wrong", "timed", "fractional", "infinite",
        "wake-first", "wake-second", "fifo", "fifo-other", "duplicate",
        "changed", "fallback", "pipeline", "pipeline-timeout", "cancelled",
    )
    keys = {name: prefix + name.encode("latin1") for name in names}
    try:
        with contextlib.ExitStack() as resources:
            def connect():
                client = Client(address)
                client.socket.settimeout(IO_TIMEOUT)
                resources.callback(client.close)
                return client

            control = connect()
            check(control, ["DEL", *keys.values()], 0)
            check(control, ["PING"], b"PONG")

            binary = keys["binary\x00\xff\r\n"]
            left, middle, right = b"a\x00", b"\xff\r\n", b"\x00last"
            check(control, ["LPUSH", binary, left, middle, b""], 3)
            check(control, ["RPUSH", binary, b"tail", right], 5)
            values = [b"", middle, left, b"tail", right]
            for start, stop, expected in (
                (0, -1, values), (1, 3, values[1:4]), (-3, -1, values[-3:]),
                (-99, 99, values), (0, -99, []), (3, 1, []), (99, 100, []),
                (-2, -2, [b"tail"]), (-9223372036854775808, 9223372036854775807, values),
            ):
                check(control, ["LRANGE", binary, start, stop], expected)
            check(control, ["LLEN", binary], 5)
            check(control, ["LPOP", binary], b"")
            check(control, ["RPOP", binary], right)
            check(control, ["RPOP", binary, 2], [b"tail", left])
            check(control, ["LPOP", binary, 9223372036854775807], [middle])
            check(control, ["TYPE", binary], b"none")
            check(control, ["EXISTS", binary], 0)
            check(control, ["LLEN", binary], 0)
            check(control, ["LRANGE", binary, 0, -1], [])
            check(control, ["LPOP", binary], None)
            check(control, ["RPOP", binary, 3], None)
            print(f"PASS {label}: binary list keys/values, push/pop order, inclusive/negative/clamped ranges, empty-key deletion")

            counted = keys["count"]
            check(control, ["RPUSH", counted, "one", "two", "three"], 3)
            check(control, ["LPOP", counted, 0], [])
            check(control, ["RPOP", counted, 0], [])
            for args in (
                ["LPOP", counted, -1], ["RPOP", counted, "bad"],
                ["LPOP", counted, 9223372036854775808], ["RPOP", counted, 1, 2],
                ["LRANGE", counted, "bad", 1], ["LRANGE", counted, 0, "bad"],
                ["LRANGE", counted, 0], ["LPUSH", counted], ["LLEN", counted, 1],
            ):
                check_error(control, args)
                check(control, ["LRANGE", counted, 0, -1], [b"one", b"two", b"three"])
            check(control, ["RPOP", counted, 1], [b"three"])
            check(control, ["LPOP", counted, 20], [b"one", b"two"])
            check(control, ["TYPE", counted], b"none")
            check(control, ["LPOP", counted, 0], None)
            print(f"PASS {label}: count pops (zero, one, excess), missing-list nulls, malformed count/range/arity errors preserve lists")

            typed, string, missing = keys["typed"], keys["string"], keys["missing"]
            check(control, ["TYPE", missing], b"none")
            check(control, ["SET", string, "42"], b"OK")
            check(control, ["TYPE", string], b"string")
            check(control, ["RPUSH", typed, "a", "b"], 2)
            check(control, ["TYPE", typed], b"list")
            for args in (["GET", typed], ["SET", typed, "new", "GET"], ["INCR", typed]):
                check_error(control, args, b"WRONGTYPE")
                check(control, ["LRANGE", typed, 0, -1], [b"a", b"b"])
            check(control, ["MGET", typed, string, missing, typed], [None, b"42", None, None])
            check(control, ["SET", typed, "replacement"], b"OK")
            check(control, ["TYPE", typed], b"string")
            for args in (
                ["LPUSH", typed, "x"], ["RPUSH", typed, "x"], ["LPOP", typed],
                ["RPOP", typed, 1], ["LLEN", typed], ["LRANGE", typed, 0, -1],
            ):
                check_error(control, args, b"WRONGTYPE")
                check(control, ["GET", typed], b"replacement")
            print(f"PASS {label}: TYPE transitions, GET/SET GET/INCR WRONGTYPE, MGET list nulls, SET replaces lists")

            ttl_key = keys["ttl"]
            check(control, ["RPUSH", ttl_key, "a", "b", "c"], 3)
            check(control, ["PEXPIRE", ttl_key, 5000], 1)
            before = control.command("PTTL", ttl_key)
            assert 0 < before <= 5000, before
            check(control, ["LPUSH", ttl_key, "front"], 4)
            check(control, ["LPOP", ttl_key], b"front")
            check(control, ["RPOP", ttl_key, 1], [b"c"])
            remaining = control.command("PTTL", ttl_key)
            assert 0 < remaining <= before, (before, remaining)
            check(control, ["LRANGE", ttl_key, 0, -1], [b"a", b"b"])
            check(control, ["PEXPIRE", ttl_key, 60], 1)
            deadline = time.monotonic() + 3
            while control.command("TYPE", ttl_key) != b"none":
                assert time.monotonic() < deadline, "list did not expire"
                time.sleep(0.005)
            check(control, ["LLEN", ttl_key], 0)
            check(control, ["PTTL", ttl_key], -2)
            check(control, ["RPUSH", ttl_key, "new"], 1)
            check(control, ["PTTL", ttl_key], -1)
            check(control, ["PEXPIRE", ttl_key, 5000], 1)
            check(control, ["LPOP", ttl_key], b"new")
            check(control, ["PTTL", ttl_key], -2)
            check(control, ["RPUSH", ttl_key, "unexpired"], 1)
            check(control, ["PTTL", ttl_key], -1)
            print(f"PASS {label}: list mutation retains TTL, lists expire, last pop removes TTL, recreated keys are persistent")

            first, second, wrong = keys["first"], keys["second"], keys["wrong"]
            check(control, ["RPUSH", first, "first-a", "first-b"], 2)
            check(control, ["RPUSH", second, "second-a", "second-b"], 2)
            check(control, ["SET", wrong, "string"], b"OK")
            check(control, ["BLPOP", first, second, wrong, 0], [first, b"first-a"])
            check(control, ["BRPOP", first, second, wrong, 0], [first, b"first-b"])
            check(control, ["BLPOP", missing, second, wrong, 0], [second, b"second-a"])
            check_error(control, ["BRPOP", missing, wrong, second, 0], b"WRONGTYPE")
            check(control, ["LRANGE", second, 0, -1], [b"second-b"])
            for args in (
                ["BLPOP"], ["BRPOP", second], ["BLPOP", second, "bad"],
                ["BRPOP", second, -1], ["BLPOP", second, "NaN"],
            ):
                check_error(control, args)
                check(control, ["LRANGE", second, 0, -1], [b"second-b"])
            check(control, ["BRPOP", second, second, 0], [second, b"second-b"])
            check(control, ["TYPE", second], b"none")
            print(f"PASS {label}: immediate BLPOP/BRPOP key priority, missing/duplicate keys, ordered WRONGTYPE, timeout validation before mutation")

            timeout_client = connect()
            timed = keys["timed"]
            exact_timeout(timeout_client, ["BLPOP", timed, "0.03"])
            check(control, ["RPUSH", timed, "after-timeout"], 1)
            check(control, ["LLEN", timed], 1)
            check(control, ["LPOP", timed], b"after-timeout")
            exact_timeout(timeout_client, ["BRPOP", timed, "0.03"])
            exact_timeout(timeout_client, ["BLPOP", keys["fractional"], "0.0001"])
            check(control, ["RPUSH", keys["fractional"], "not-stolen"], 1)
            check(control, ["LRANGE", keys["fractional"], 0, -1], [b"not-stolen"])
            print(f"PASS {label}: exact RESP null-array timeout frames, fractional/submillisecond finite timeout, timed-out waiters cannot consume later pushes")

            infinite = connect()
            begin_wait(infinite, ["BRPOP", keys["infinite"], 0], control)
            check(control, ["SET", string, "responsive"], b"OK")
            check(control, ["GET", string], b"responsive")
            check(control, ["RPUSH", keys["infinite"], "left", "right"], 2)
            actual = infinite.read()
            assert actual == [keys["infinite"], b"right"], actual
            check(control, ["LRANGE", keys["infinite"], 0, -1], [b"left"])
            print(f"PASS {label}: zero-timeout BRPOP waits indefinitely without blocking other clients and wakes from the right")

            multi = connect()
            wake_first, wake_second = keys["wake-first"], keys["wake-second"]
            begin_wait(multi, ["BLPOP", wake_first, wake_second, 0], control)
            check(control, ["LPUSH", wake_second, "second-key"], 1)
            actual = multi.read()
            assert actual == [wake_second, b"second-key"], actual
            check(control, ["TYPE", wake_first], b"none")
            check(control, ["TYPE", wake_second], b"none")
            print(f"PASS {label}: a multi-key blocked pop wakes from its second key")

            fifo, fifo_other = keys["fifo"], keys["fifo-other"]
            waiters = [connect(), connect(), connect()]
            begin_wait(waiters[0], ["BLPOP", fifo, fifo_other, 0], control)
            begin_wait(waiters[1], ["BRPOP", fifo, 0], control)
            begin_wait(waiters[2], ["BLPOP", fifo, 0], control)
            check(control, ["RPUSH", fifo, "a", "b", "c", "d"], 4)
            actual = [waiter.read() for waiter in waiters]
            assert actual == [[fifo, b"a"], [fifo, b"d"], [fifo, b"b"]], actual
            check(control, ["LRANGE", fifo, 0, -1], [b"c"])
            check(control, ["RPUSH", fifo_other, "kept"], 1)
            check(waiters[0], ["PING"], b"PONG")
            check(control, ["LRANGE", fifo_other, 0, -1], [b"kept"])
            print(f"PASS {label}: waiting-client FIFO, mixed pop ends, multi-element push replies with pre-consumer length, completed multi-key waiters are removed")

            duplicate = connect()
            dupkey = keys["duplicate"]
            begin_wait(duplicate, ["BLPOP", dupkey, dupkey, 0], control)
            check(control, ["RPUSH", dupkey, "once"], 1)
            actual = duplicate.read()
            assert actual == [dupkey, b"once"], actual
            check(control, ["RPUSH", dupkey, "kept"], 1)
            check(duplicate, ["PING"], b"PONG")
            check(control, ["LRANGE", dupkey, 0, -1], [b"kept"])
            print(f"PASS {label}: duplicate watched keys deliver once and leave no duplicate stale waiter")

            changed = connect()
            watched, fallback = keys["changed"], keys["fallback"]
            begin_wait(changed, ["BLPOP", watched, fallback, 0], control)
            check(control, ["SET", watched, "now-a-string"], b"OK")
            expect_quiet(changed)
            check(control, ["PING"], b"PONG")
            check(control, ["RPUSH", fallback, "fallback-value"], 1)
            actual = changed.read()
            assert isinstance(actual, tuple) and actual[0] == "error" and actual[1].split(b" ", 1)[0] == b"WRONGTYPE", actual
            check(control, ["GET", watched], b"now-a-string")
            check(control, ["LRANGE", fallback, 0, -1], [b"fallback-value"])
            check_error(changed, ["BLPOP", watched, fallback, 0], b"WRONGTYPE")
            print(f"PASS {label}: SET alone does not wake a waiter; a later list push rechecks key order, returns WRONGTYPE, and retains the unconsumed item")

            pipeline = connect()
            pipeline.socket.sendall(
                Client.frame(["BLPOP", keys["pipeline"], 0]) + Client.frame(["PING"])
            )
            expect_quiet(pipeline)
            check(control, ["PING"], b"PONG")
            check(control, ["RPUSH", keys["pipeline"], "payload"], 1)
            actual = [pipeline.read(), pipeline.read()]
            assert actual == [[keys["pipeline"], b"payload"], b"PONG"], actual
            pipeline.socket.sendall(
                Client.frame(["BLPOP", keys["pipeline-timeout"], "0.03"])
                + Client.frame(["PING"])
            )
            actual = pipeline.file.readline()
            assert actual == b"*-1\r\n", actual
            actual = pipeline.read()
            assert actual == b"PONG", actual
            print(f"PASS {label}: BLPOP+PING pipeline preserves response order after both wake and timeout")

            cancelled = connect()
            cancelkey = keys["cancelled"]
            begin_wait(cancelled, ["BLPOP", cancelkey, 0], control)
            cancelled.socket.shutdown(socket.SHUT_RDWR)
            cancelled.close()
            # EOF travels through a different connection task. No protocol
            # command can acknowledge another connection's cancellation; allow
            # one bounded scheduling interval before testing a later push.
            time.sleep(0.15)
            check(control, ["PING"], b"PONG")
            check(control, ["RPUSH", cancelkey, "survives-disconnect"], 1)
            check(control, ["LRANGE", cancelkey, 0, -1], [b"survives-disconnect"])
            check(control, ["BLPOP", cancelkey, "0.2"], [cancelkey, b"survives-disconnect"])
            check(control, ["RPUSH", cancelkey, "still-not-stolen"], 1)
            check(control, ["LRANGE", cancelkey, 0, -1], [b"still-not-stolen"])
            print(f"PASS {label}: disconnected blocked clients cannot consume future pushes and leave no stale waiter")
    finally:
        cleanup_keys(address, keys.values())


def state(client, keys):
    result = {}
    for key in keys:
        kind = client.command("TYPE", key)
        assert kind in (b"none", b"string", b"list"), (key, kind)
        if kind == b"string":
            value = client.command("GET", key)
            assert isinstance(value, bytes), (key, kind, value)
        elif kind == b"list":
            value = client.command("LRANGE", key, 0, -1)
            assert isinstance(value, list) and value, (key, kind, value)
        else:
            value = None
        result[key] = (kind, value)
    return result


def differential(address, reference, prefix):
    seed, cases = 81173, 1600
    rng = random.Random(seed)
    keys = [prefix + b"random:%d" % index for index in range(8)]
    values = [b"", b"0", b"42", b"-9", b"01", b"\x00\xff\r\n", b"9223372036854775807"]
    ranges = [-9223372036854775808, -99, -3, -1, 0, 1, 3, 99, 9223372036854775807]
    try:
        with contextlib.ExitStack() as resources:
            clients = []
            for endpoint in (address, reference):
                client = Client(endpoint)
                resources.callback(client.close)
                clients.append(client)
                check(client, ["DEL", *keys], 0)
            # Cycling the operation templates guarantees malformed options,
            # counts, ranges, arities and duplicate-key commands are exercised;
            # keys, values, counts and transitions remain seeded/randomized.
            for index in range(cases):
                key, other = rng.choice(keys), rng.choice(keys)
                args = [
                    ["LPUSH", key, rng.choice(values), rng.choice(values)],
                    ["RPUSH", key, rng.choice(values), rng.choice(values)],
                    ["LPOP", key], ["RPOP", key, rng.choice([0, 1, 2, 99])],
                    ["LLEN", key], ["LRANGE", key, rng.choice(ranges), rng.choice(ranges)],
                    ["DEL", key, key, other], ["EXISTS", key, key, other], ["TYPE", key],
                    ["SET", key, rng.choice(values)], ["SET", key, "9", "GET"],
                    ["SET", key, "10", "NX", "GET"], ["SET", key, "11", "XX", "GET"],
                    ["SET", key, "12", "BADOPTION"], ["SET", key, "13", "PX", "bad"],
                    ["SET", key, "14", "NX", "XX"], ["GET", key],
                    ["MGET", key, other, key], ["MSET", key, "5", key, "7", other, "9"],
                    ["INCR", key], ["INCRBY", key, rng.randint(-50, 50)],
                    ["DECRBY", key, rng.randint(-50, 50)],
                    ["LPOP", key, -1], ["RPOP", key, "bad"], ["LPOP", key, 1, 2],
                    ["LRANGE", key, "bad", 1], ["LRANGE", key, 0, "bad"],
                    ["LRANGE", key, 0], ["LPUSH", key], ["RPUSH", key], ["LLEN", key, 1],
                    ["BLPOP", key, key, -1], ["BRPOP", key, key, "bad"],
                    ["LPOP", key, 9223372036854775808],
                    ["LRANGE", key, -9223372036854775809, 0],
                    ["SET", key, "42", "KEEPTTL"], ["PEXPIREAT", key, 1],
                    ["PERSIST", key], ["MSET", key, "2", other],
                ][index % 39]
                touched = list(dict.fromkeys([key, other]))
                before = [state(client, touched) for client in clients]
                assert before[0] == before[1], (seed, index, args, "before", before)
                actual, expected = [client.command(*args) for client in clients]
                if isinstance(expected, tuple) and expected[0] == "error":
                    assert isinstance(actual, tuple) and actual[0] == "error", (seed, index, args, actual, expected)
                    # Error categories affect callers; prose is not a compatibility contract.
                    assert actual[1].split(b" ", 1)[0] == expected[1].split(b" ", 1)[0], (seed, index, args, actual, expected)
                else:
                    assert actual == expected, (seed, index, args, actual, expected)
                after = [state(client, touched) for client in clients]
                assert after[0] == after[1], (seed, index, args, "after", after)
                if isinstance(expected, tuple) and expected[0] == "error":
                    assert after == before, (seed, index, args, "error mutated state", before, after)
            final = [state(client, keys) for client in clients]
            assert final[0] == final[1], (seed, "final state", final)
        print(f"PASS reference: {cases} differential list/string operations (seed {seed}), duplicate keys, malformed options/counts/ranges, post-command state and error mutation invariants")
    finally:
        cleanup_keys(address, keys)
        cleanup_keys(reference, keys)


def startup_address(process):
    deadline = time.monotonic() + IO_TIMEOUT
    captured = b""
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0, ("ffkv startup timed out", captured)
        readable, _, _ = select.select([process.stderr], [], [], remaining)
        assert readable, ("ffkv startup timed out", captured)
        chunk = os.read(process.stderr.fileno(), 4096)
        assert chunk, ("ffkv exited before startup", process.poll(), captured)
        captured += chunk
        assert len(captured) <= 65536, ("unexpected startup output", captured)
        for line in captured.split(b"\n")[:-1]:
            if b"listening on " in line:
                address = line.split(b"listening on ", 1)[1].split()[0].decode()
                host, port = address.rsplit(":", 1)
                return host, int(port)


def parse_address(value):
    host, port = value.rsplit(":", 1)
    return host, int(port)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference", type=parse_address, help="Disposable Redis host:port; writes isolated test keys")
    options = parser.parse_args()
    prefix = b"ffkv:test:queue:" + uuid.uuid4().hex.encode() + b":"
    process = subprocess.Popen(
        [options.binary, "--bind", "127.0.0.1:0"],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )
    try:
        address = startup_address(process)
        exercise(address, prefix + b"scenarios:", "ffkv")
        if options.reference:
            exercise(options.reference, prefix + b"scenarios:", "reference")
            differential(address, options.reference, prefix)
    finally:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=IO_TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=IO_TIMEOUT)
        if process.stderr:
            process.stderr.close()
    assert process.returncode == 0, ("ffkv shutdown", process.returncode)
    print("PASS ffkv queue smoke complete; isolated process exited cleanly on SIGINT")


if __name__ == "__main__":
    main()
