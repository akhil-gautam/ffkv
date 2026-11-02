#!/usr/bin/env python3
"""Exercise shard coordination and shard-count-independent recovery on real ffkv.

Usage: python3 tests/shards_smoke.py [--binary target/debug/ffkv]

The workflow owns every server and temporary data directory. It runs in-memory
with --shards 1 and 4, then crashes a strict persistent server while reopening
4 -> 1 -> 4 shards. Cursors are opaque, set choices are checked by membership,
and many distinct keys exercise routing without assuming a shard hash function.
SIGKILL exercises process-crash recovery, not physical power-loss durability.
Only small payloads are needed: the large atomic fixture has 1,400 set members,
not a large-memory or performance workload. No Redis installation is required.
"""
import argparse
import concurrent.futures
import signal
import tempfile
import threading
import time

from durability_smoke import (
    Server, check_deadline, current_ms, passed, wait_until_ms,
)
from hashes_smoke import pairs
from queue_smoke import IO_TIMEOUT, begin_wait, check, expect_quiet
from sets_smoke import check_error, member_set, normalized


PREFIX = b"ffkv:test:shards:"
WORK_TIMEOUT = 30


def scan_all(client, match=None, kind=None, mutate=None):
    """Follow only server-issued cursors; COUNT is a hint, not a page size."""
    cursor = b"0"
    found = set()
    deadline = time.monotonic() + WORK_TIMEOUT
    for iteration in range(10000):
        assert time.monotonic() < deadline, "SCAN exceeded its watchdog"
        args = ["SCAN", cursor, "COUNT", 1]
        if match is not None:
            args += ["MATCH", match]
        if kind is not None:
            args += ["TYPE", kind]
        reply = client.command(*args)
        assert isinstance(reply, list) and len(reply) == 2, reply
        cursor, values = reply
        assert isinstance(cursor, bytes) and isinstance(values, list), reply
        assert all(isinstance(key, bytes) for key in values), reply
        found.update(values)
        if mutate is not None:
            mutate(iteration)
        if cursor == b"0":
            return found
    raise AssertionError("SCAN did not finish within the bounded iteration count")


def atomic_mset(server):
    control = server.connect()
    keys = [PREFIX + b"atomic:%03d" % index for index in range(96)]
    initial = b"initial"
    args = ["MSET"]
    for key in keys:
        args.extend((key, initial))
    check(control, args, b"OK")

    writers = 3
    readers = 2
    rounds = 100
    generations = {initial} | {
        b"writer:%d:generation:%d" % (writer, generation)
        for writer in range(writers) for generation in range(rounds)
    }
    clients = [server.connect() for _ in range(writers + readers)]
    barrier = threading.Barrier(writers + readers)
    stop = threading.Event()
    writers_done = threading.Event()
    finished = 0
    finish_lock = threading.Lock()
    deadline = time.monotonic() + WORK_TIMEOUT

    def writer(index, client):
        nonlocal finished
        try:
            barrier.wait(timeout=IO_TIMEOUT)
            for generation in range(rounds):
                if stop.is_set():
                    return
                assert time.monotonic() < deadline, "MSET writer exceeded watchdog"
                value = b"writer:%d:generation:%d" % (index, generation)
                order = keys if (index + generation) % 2 else keys[::-1]
                # Every key occurs twice. Only the final occurrence may become
                # visible, even to a concurrent reader spanning all shards.
                command = ["MSET"]
                for key in order:
                    command.extend((key, b"must-never-be-visible"))
                for key in order[::-1]:
                    command.extend((key, value))
                check(client, command, b"OK")
        finally:
            with finish_lock:
                finished += 1
                if finished == writers:
                    writers_done.set()

    def reader(client):
        barrier.wait(timeout=IO_TIMEOUT)
        reads = 0
        query = keys[::-1] + keys[:17] + keys[::7]
        while not stop.is_set() and (not writers_done.is_set() or reads < 200):
            assert time.monotonic() < deadline, "MGET reader exceeded watchdog"
            reply = client.command("MGET", *query)
            assert isinstance(reply, list) and len(reply) == len(query), reply
            value = reply[0]
            assert value in generations and all(item == value for item in reply), (
                "MGET observed a torn or intermediate MSET generation", reply,
            )
            reads += 1
        return reads

    with concurrent.futures.ThreadPoolExecutor(max_workers=writers + readers) as pool:
        futures = [pool.submit(writer, index, clients[index]) for index in range(writers)]
        futures += [pool.submit(reader, client) for client in clients[writers:]]
        try:
            results = [future.result(timeout=WORK_TIMEOUT + IO_TIMEOUT) for future in futures]
        finally:
            stop.set()
            barrier.abort()
    assert all(reads >= 200 for reads in results[writers:]), results[writers:]
    reply = control.command("MGET", *keys, *keys[::-1])
    assert len(reply) == 2 * len(keys) and reply[0] != initial, reply
    assert reply[0] in generations and all(value == reply[0] for value in reply), reply
    check(control, ["DEL", *keys], len(keys))
    passed("concurrent overlapping MSET/MGET: whole-generation views, duplicate keys and reversed lock order")


def verify_views(client, values, key_deadlines, field_deadlines):
    """Assert consumer-visible values as well as coordinated whole-keyspace views."""
    for key, (kind, value) in values.items():
        check(client, ["TYPE", key], kind)
        if kind == b"string":
            check(client, ["GET", key], value)
        elif kind == b"list":
            check(client, ["LRANGE", key, 0, -1], value)
            check(client, ["LLEN", key], len(value))
            for index, expected in enumerate(value):
                check(client, ["LINDEX", key, index], expected)
                check(client, ["LINDEX", key, index - len(value)], expected)
            check(client, ["LINDEX", key, len(value)], None)
            check(client, ["LINDEX", key, -len(value) - 1], None)
        elif kind == b"hash":
            assert pairs(client.command("HGETALL", key)) == value, key
        elif kind == b"set":
            assert member_set(client, key) == value, key
        elif kind == b"zset":
            check(client, ["ZRANGE", key, 0, -1, "WITHSCORES"], value)
        else:
            raise AssertionError(("unsupported fixture type", key, kind))
    for key, deadline in key_deadlines.items():
        check_deadline(client, key, deadline)
    for (key, field), deadline in field_deadlines.items():
        check(client, ["HPEXPIRETIME", key, "FIELDS", 1, field], [deadline])

    keys = list(values)
    expected = set(keys)
    check(client, ["DBSIZE"], len(expected))
    check(client, ["EXISTS", *keys, keys[0]], len(keys) + 1)
    assert scan_all(client) == expected, "SCAN did not visit the complete keyspace"
    assert scan_all(client, match=PREFIX + b"*") == expected, "SCAN MATCH lost keys"
    for kind in (b"string", b"list", b"hash", b"set", b"zset"):
        typed = {key for key, (actual, _) in values.items() if actual == kind}
        assert scan_all(client, kind=kind) == typed, ("SCAN TYPE", kind)
        assert scan_all(client, match=PREFIX + b"*", kind=kind) == typed, (
            "SCAN MATCH TYPE", kind,
        )
    query = keys + keys[::-1]
    check(client, ["MGET", *query], [
        values[key][1] if values[key][0] == b"string" else None for key in query
    ])


def typed_views(server):
    client = server.connect()
    values = {}
    key_deadlines = {}
    field_deadlines = {}
    absolute = current_ms() + 120000
    for index in range(8):
        suffix = b"%02d" % index
        for kind in (b"string", b"list", b"hash", b"set", b"zset"):
            source = PREFIX + b"original:" + kind + b":" + suffix
            target = PREFIX + b"moved:" + kind + b":" + suffix
            if index == 0:
                # Routing and coordinated scans must not treat binary names as
                # string delimiters. These suffixes contain no glob operators.
                source += b"\x00\xff\r\n"
                target += b"\x00\xff\r\n"
            if kind == b"string":
                value = b"value\x00\xff\r\n:" + suffix
                check(client, ["SET", source, value], b"OK")
            elif kind == b"list":
                check(client, ["RPUSH", source, b"discard", b"left\x00", b"old", b"right\xff"], 4)
                check(client, ["LSET", source, -2, b"middle:" + suffix], b"OK")
                check(client, ["LTRIM", source, 1, -1], b"OK")
                value = [b"left\x00", b"middle:" + suffix, b"right\xff"]
            elif kind == b"hash":
                value = {b"permanent": b"keep:" + suffix, b"field\x00\xff": b"timed:" + suffix}
                fields = [item for pair in value.items() for item in pair]
                check(client, ["HSET", source, *fields], 2)
                check(client, ["HPEXPIREAT", source, absolute, "FIELDS", 1, b"field\x00\xff"], [1])
            elif kind == b"set":
                value = {b"member\x00", b"member\xff", suffix}
                check(client, ["SADD", source, *sorted(value)], len(value))
            else:
                value = [b"low\x00:" + suffix, b"1", b"high\xff:" + suffix, b"2"]
                check(client, ["ZADD", source, 1, value[0], 2, value[2]], 2)
            check(client, ["PEXPIREAT", source, absolute], 1)
            # Both a failed NX move and a successful replacement cross distinct
            # names. Replacing a different type must remove its old indices/TTL.
            if kind == b"string":
                check(client, ["RPUSH", target, "discarded-list"], 1)
                target_kind = b"list"
            else:
                check(client, ["SET", target, "discarded-string"], b"OK")
                target_kind = b"string"
            check(client, ["PEXPIREAT", target, absolute + 10000], 1)
            check(client, ["RENAMENX", source, target], 0)
            check(client, ["TYPE", source], kind)
            check(client, ["TYPE", target], target_kind)
            check_deadline(client, source, absolute)
            check_deadline(client, target, absolute + 10000)
            if index % 2:
                check(client, ["DEL", target], 1)
                check(client, ["RENAMENX", source, target], 1)
            else:
                check(client, ["RENAME", source, target], b"OK")
            check(client, ["TYPE", source], b"none")
            check(client, ["PTTL", source], -2)
            check(client, ["RENAMENX", target, target], 0)
            check(client, ["RENAME", target, target], b"OK")
            values[target] = (kind, value)
            key_deadlines[target] = absolute
            if kind == b"hash":
                field_deadlines[target, b"field\x00\xff"] = absolute
    missing = PREFIX + b"missing-source"
    check_error(client, ["RENAME", missing, next(iter(values))])
    check_error(client, ["RENAMENX", missing, next(iter(values))])
    verify_views(client, values, key_deadlines, field_deadlines)
    string_keys = {key for key, (kind, _) in values.items() if kind == b"string"}
    assert scan_all(client, match=PREFIX + b"moved:string:??*") == string_keys
    passed("RENAME/RENAMENX preserve all five types, binary data, key/field TTLs and list indices; global DBSIZE/SCAN/MATCH/TYPE agree")
    return values, key_deadlines, field_deadlines


def overlapping_sets(server):
    client = server.connect()
    owned = []
    for index in range(12):
        a, b, wrong = [PREFIX + b"algebra:%02d:" % index + suffix for suffix in (b"a", b"b", b"wrong")]
        owned += [a, b, wrong]
        check(client, ["SADD", a, "a", "b", "c"], 3)
        check(client, ["SADD", b, "c", "d"], 2)
        check(client, ["SET", wrong, "unchanged"], b"OK")
        deadline = current_ms() + 60000
        for key in (a, b):
            check(client, ["PEXPIREAT", key, deadline], 1)
        check_error(client, ["SMOVE", a, wrong, "b"], b"WRONGTYPE")
        check_error(client, ["SINTERSTORE", a, b, wrong], b"WRONGTYPE")
        assert member_set(client, a) == {b"a", b"b", b"c"}
        assert member_set(client, b) == {b"c", b"d"}
        check(client, ["GET", wrong], b"unchanged")
        check(client, ["SMOVE", a, a, "b"], 1)
        check(client, ["SMOVE", a, a, "absent"], 0)
        check(client, ["SMOVE", a, b, "a"], 1)
        assert member_set(client, a) == {b"b", b"c"}
        assert member_set(client, b) == {b"a", b"c", b"d"}
        for key in (a, b):
            check_deadline(client, key, deadline)
        check(client, ["SUNIONSTORE", a, a, b, b], 4)
        assert member_set(client, a) == {b"a", b"b", b"c", b"d"}
        check(client, ["PTTL", a], -1)
        check(client, ["SINTERSTORE", b, a, b], 3)
        assert member_set(client, b) == {b"a", b"c", b"d"}
        check(client, ["PTTL", b], -1)
        check(client, ["SDIFFSTORE", a, a, b], 1)
        assert member_set(client, a) == {b"b"}
        check(client, ["SMOVE", a, b, "b"], 1)
        check(client, ["TYPE", a], b"none")
        assert member_set(client, b) == {b"a", b"b", b"c", b"d"}
        check(client, ["SDIFFSTORE", b, b, b], 0)
        check(client, ["TYPE", b], b"none")
        check(client, ["SUNIONSTORE", wrong, a, b], 0)
        check(client, ["TYPE", wrong], b"none")
    check(client, ["DEL", *owned], 0)
    passed("SMOVE and all algebra STORE commands coordinate overlapping/repeated sources and destinations, retain/reset TTLs and preserve state on WRONGTYPE")


def blocking_views(server):
    control = server.connect()
    keys = [PREFIX + b"blocking:%02d" % index for index in range(32)]
    waiters = [server.connect() for _ in range(3)]
    begin_wait(waiters[0], ["BLPOP", *keys, 0], control)
    begin_wait(waiters[1], ["BRPOP", *keys[::-1], 0], control)
    begin_wait(waiters[2], ["BLPOP", *keys, 0], control)
    selected = keys[19]
    check(control, ["RPUSH", selected, "a", "b", "c", "d"], 4)
    replies = [client.read() for client in waiters]
    assert replies == [[selected, b"a"], [selected, b"d"], [selected, b"b"]], replies
    check(control, ["LRANGE", selected, 0, -1], [b"c"])
    check(control, ["DEL", selected], 1)
    check(control, ["RPUSH", keys[3], "not-stolen"], 1)
    for client in waiters:
        check(client, ["PING"], b"PONG")
    check(control, ["LPOP", keys[3]], b"not-stolen")

    donor = PREFIX + b"blocking:donor"
    for command, selected in (("RENAME", keys[-1]), ("RENAMENX", keys[11])):
        waiting = server.connect()
        begin_wait(waiting, ["BLPOP", *keys, 0], control)
        check(control, ["RPUSH", donor, "left", "right"], 2)
        check(control, [command, donor, selected], b"OK" if command == "RENAME" else 1)
        reply = waiting.read()
        assert reply == [selected, b"left"], reply
        check(control, ["RPOP", selected], b"right")
        check(control, ["TYPE", donor], b"none")

    changed = server.connect()
    begin_wait(changed, ["BRPOP", *keys, 0], control)
    check(control, ["SET", keys[0], "now-string"], b"OK")
    expect_quiet(changed)
    check(control, ["RPUSH", keys[-1], "unconsumed"], 1)
    reply = changed.read()
    assert normalized(reply) == ("error", b"WRONGTYPE"), reply
    check(control, ["GET", keys[0]], b"now-string")
    check(control, ["LRANGE", keys[-1], 0, -1], [b"unconsumed"])
    check(control, ["DEL", keys[0]], 1)
    check(control, ["BRPOP", *keys, 0], [keys[-1], b"unconsumed"])

    # Key argument order wins over creation order and shard visitation order.
    check(control, ["RPUSH", keys[-1], "last-left", "last-right"], 2)
    check(control, ["RPUSH", keys[0], "first-left", "first-right"], 2)
    check(control, ["BLPOP", *keys, 0], [keys[0], b"first-left"])
    check(control, ["BRPOP", *keys, 0], [keys[0], b"first-right"])
    check(control, ["BRPOP", *keys, 0], [keys[-1], b"last-right"])
    check(control, ["LPOP", keys[-1]], b"last-left")
    check(control, ["DEL", *keys, donor], 0)
    passed("32-key BLPOP/BRPOP preserve waiter FIFO, key priority, rename wakeups and type-change rechecks without stale consumption")


def stable_scan(server):
    scanner = server.connect()
    mutator = server.connect()
    stable = {PREFIX + b"scan:stable:%03d" % index for index in range(96)}
    transient = {PREFIX + b"scan:transient:initial:%02d" % index for index in range(8)}
    for group in (stable, transient):
        args = ["MSET"]
        for key in sorted(group):
            args.extend((key, b"value"))
        check(mutator, args, b"OK")

    def mutate(iteration):
        nonlocal transient
        check(mutator, ["DEL", *sorted(transient)], len(transient))
        transient = {
            PREFIX + b"scan:transient:%05d:%02d" % (iteration, index)
            for index in range(8)
        }
        args = ["MSET"]
        for key in sorted(transient):
            args.extend((key, b"value"))
        check(mutator, args, b"OK")

    found = scan_all(scanner, mutate=mutate)
    assert stable <= found, ("continuously present keys skipped", stable - found)
    assert scan_all(scanner, match=PREFIX + b"scan:stable:*") == stable
    assert scan_all(scanner) == stable | transient
    check(scanner, ["DBSIZE"], len(stable) + len(transient))
    check(mutator, ["DEL", *sorted(stable | transient)], len(stable) + len(transient))
    passed("opaque COUNT 1 SCAN retains every continuous key while a second client replaces transient keys between pages")


def in_memory(binary, shards):
    with Server(binary, extra_args=("--shards", str(shards))) as server:
        atomic_mset(server)
        values, _, _ = typed_views(server)
        check(server.connect(), ["DEL", *values], len(values))
        overlapping_sets(server)
        blocking_views(server)
        stable_scan(server)
        check(server.connect(), ["DBSIZE"], 0)
    passed(f"isolated in-memory --shards {shards} acceptance complete")


def crash_repartition(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-shards-crash-") as directory:
        with Server(binary, directory, durability="strict", extra_args=("--shards", "4")) as server:
            values, key_deadlines, field_deadlines = typed_views(server)
            client = server.connect()
            left_key, right_key, store_key = [
                PREFIX + b"large:" + name for name in (b"left", b"right", b"store")
            ]
            left = {b"left:%04d" % index for index in range(700)}
            right = {b"right:%04d" % index for index in range(700)}
            universe = left | right
            # Each input fits the RESP argument bound, while STORE and SPOP
            # resolve more than the historical 1,024-row atomic WAL limit.
            # Even counting source/destination copies, payloads are < 64 KiB.
            assert 3 * sum(map(len, universe)) < 64 * 1024
            check(client, ["SADD", left_key, *sorted(left)], len(left))
            check(client, ["SADD", right_key, *sorted(right)], len(right))
            check(client, ["SUNIONSTORE", store_key, left_key, right_key], 1400)
            values[left_key] = (b"set", left)
            values[right_key] = (b"set", right)
            values[store_key] = (b"set", universe)

            offline_at = current_ms() + 2000
            hash_keys = [key for key, (kind, _) in values.items() if kind == b"hash"]
            for key in hash_keys:
                check(client, ["HSETEX", key, "PXAT", offline_at, "FIELDS", 1, "offline", "must-not-return"], 1)
            expired_only = PREFIX + b"offline:last-field"
            check(client, ["HSETEX", expired_only, "PXAT", offline_at, "FIELDS", 1, "gone", "must-not-return"], 1)
            watched = [PREFIX + b"durable:watched:%02d" % index for index in range(32)]
            waiter = server.connect()
            begin_wait(waiter, ["BRPOP", *watched, 0], client)
            check(client, ["RPUSH", watched[-1], "retained-left", "delivered-right"], 2)
            reply = waiter.read()
            assert reply == [watched[-1], b"delivered-right"], reply
            values[watched[-1]] = (b"list", [b"retained-left"])
            # No command barrier after the delivered pop. Crash recovery must
            # retain both the large STORE and the acknowledged blocking removal.
            server.stop(signal.SIGKILL)
            wait_until_ms(offline_at + 30)
            server.extra_args = ("--shards", "1")
            server.start()
            client = server.connect()
            verify_views(client, values, key_deadlines, field_deadlines)
            check(client, ["TYPE", expired_only], b"none")
            for key in hash_keys:
                check(client, ["HGET", key, "offline"], None)
                check(client, ["HPTTL", key, "FIELDS", 1, "offline"], [-2])
            passed("strict SIGKILL/reopen 4 -> 1: all types, TTLs, hash field deadlines, moved list indices, 1,400-row STORE and blocking pop survive")

            selected = client.command("SPOP", store_key, 1200)
            assert isinstance(selected, list) and len(selected) == 1200, selected
            popped = set(selected)
            assert len(popped) == 1200 and popped <= universe, "invalid random SPOP selection"
            remaining = universe - popped
            values[store_key] = (b"set", remaining)
            # The exact random choice, not merely its count, must be durable.
            # No state read or other command intervenes before this SIGKILL.
            server.stop(signal.SIGKILL)
            server.extra_args = ("--shards", "4")
            server.start()
            client = server.connect()
            verify_views(client, values, key_deadlines, field_deadlines)
            check(client, ["SCARD", store_key], 200)
            assert member_set(client, store_key).isdisjoint(popped), "popped members resurrected"
            check(client, ["TYPE", expired_only], b"none")
            for key in hash_keys:
                check(client, ["HGET", key, "offline"], None)
            # Recovered routing must accept new mutations, not just serve a
            # reconstructed snapshot with the old shard count as its identity.
            check(client, ["SMOVE", store_key, left_key, min(remaining)], 1)
            check(client, ["SCARD", store_key], 199)
            check(client, ["PING"], b"PONG")
    passed("strict SIGKILL/reopen 1 -> 4: exact 1,200-member SPOP result persists without resurrection; recovered keys remain mutable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    options = parser.parse_args()
    for shards in (1, 4):
        in_memory(options.binary, shards)
    crash_repartition(options.binary)
    passed("shard smoke complete; owned processes, sockets and temporary directories cleaned up")


if __name__ == "__main__":
    main()
