#!/usr/bin/env python3
"""Exercise real ffkv transactions, WATCH, blocked consumers and crash recovery.

Usage: python3 tests/transactions_smoke.py [--binary target/debug/ffkv]

Every server, socket, worker and temporary data directory belongs to this script.
Transactions run with one and four shards; concurrent tuple reads exercise many
keys without assuming a shard hash function. SIGKILL tests process-crash recovery,
not power-loss durability. No Redis installation or third-party package is needed.
"""
import argparse
import concurrent.futures
import signal
import tempfile
import threading
import time

from cli_smoke import Client
from durability_smoke import Server, current_ms, passed, wait_until_ms
from queue_smoke import IO_TIMEOUT, begin_wait, check, expect_quiet


PREFIX = b"ffkv:test:transactions:"
WORK_TIMEOUT = 30


def error_category(reply, category):
    assert (
        isinstance(reply, tuple)
        and len(reply) == 2
        and reply[0] == "error"
        and isinstance(reply[1], bytes)
        and reply[1].split(b" ", 1)[0] == category
    ), (reply, category)


def check_error(client, args, category=b"ERR"):
    reply = client.command(*args)
    error_category(reply, category)
    return reply


def queue(client, commands):
    """Pipeline actual commands, checking every queue acknowledgement."""
    commands = list(commands)
    if commands:
        client.socket.sendall(b"".join(Client.frame(args) for args in commands))
    for args in commands:
        reply = client.read()
        assert reply == b"QUEUED", (args, reply, b"QUEUED")


def execute(client, commands):
    check(client, ["MULTI"], b"OK")
    queue(client, commands)
    return client.command("EXEC")


def aborted_watch(client):
    # Client.read maps both null kinds to None; WATCH aborts must be null arrays.
    client.socket.sendall(Client.frame(["EXEC"]))
    reply = client.file.readline()
    assert reply == b"*-1\r\n", ("WATCH-aborted EXEC", reply)


def queue_and_errors(server):
    client, observer = server.connect(), server.connect()
    key, tail, items = [PREFIX + suffix for suffix in (b"queued", b"tail", b"items")]
    payload = b"binary\x00\xff\r\n$3\r\nabc"
    check(client, ["SET", key, "before"], b"OK")
    check(client, ["MULTI"], b"OK")
    queue(client, [["SET", key, payload], ["GET", key], ["RPUSH", items, "first"]])
    check(observer, ["GET", key], b"before")
    check(observer, ["EXISTS", items], 0)
    check(client, ["EXEC"], [b"OK", payload, 1])
    check(observer, ["GET", key], payload)
    check(observer, ["LRANGE", items, 0, -1], [b"first"])

    reply = execute(client, [
        ["SET", key, "written-before-error"],
        ["LPUSH", key, "wrong-type"],
        ["SET", tail, "written-after-error"],
        ["RPUSH", items, "last"],
        ["MGET", key, tail],
    ])
    assert isinstance(reply, list) and len(reply) == 5, reply
    assert reply[0] == b"OK" and reply[2:] == [
        b"OK", 2, [b"written-before-error", b"written-after-error"],
    ], reply
    error_category(reply[1], b"WRONGTYPE")
    check(observer, ["MGET", key, tail], [b"written-before-error", b"written-after-error"])
    check(observer, ["LRANGE", items, 0, -1], [b"first", b"last"])

    for invalid in (["FFKV_UNKNOWN_TRANSACTION_COMMAND"], ["SET", key]):
        check(client, ["MULTI"], b"OK")
        queue(client, [["SET", key, "must-not-be-written"]])
        check_error(client, invalid)
        queue(client, [["SET", tail, "also-must-not-be-written"], ["DEL", items]])
        check_error(client, ["EXEC"], b"EXECABORT")
        check(observer, ["MGET", key, tail], [b"written-before-error", b"written-after-error"])
        check(observer, ["LRANGE", items, 0, -1], [b"first", b"last"])
        # EXECABORT must leave neither a transaction nor dirty state behind.
        assert execute(client, [["GET", key]]) == [b"written-before-error"]

    check(client, ["MULTI"], b"OK")
    queue(client, [["SET", key, "discarded"], ["DEL", items]])
    check_error(client, ["SET", key])
    check(client, ["DISCARD"], b"OK")
    check(client, ["GET", key], b"written-before-error")
    assert execute(client, [["GET", tail], ["LLEN", items]]) == [b"written-after-error", 2]
    check_error(client, ["EXEC"])
    passed("MULTI defers changes; EXEC preserves ordered partial effects around WRONGTYPE; queue errors abort; DISCARD clears dirty state")


def watch_tokens(server):
    watcher, other = server.connect(), server.connect()
    key, unrelated, marker, added = [
        PREFIX + suffix for suffix in (b"watched", b"unrelated", b"watch-marker", b"watch-added")
    ]
    check(other, ["SET", key, "initial"], b"OK")
    check(other, ["SET", added, "initial"], b"OK")

    # Repeating WATCH and extending its key set must not refresh an old token.
    check(watcher, ["WATCH", key], b"OK")
    check(other, ["SET", key, "changed"], b"OK")
    check(watcher, ["WATCH", key, added], b"OK")
    check(watcher, ["MULTI"], b"OK")
    queue(watcher, [["SET", marker, "must-not-exist"]])
    aborted_watch(watcher)
    check(other, ["EXISTS", marker], 0)

    # A watched mutation on the watching connection also invalidates its token.
    check(watcher, ["WATCH", key], b"OK")
    check(watcher, ["SET", key, "same-connection-change"], b"OK")
    check(watcher, ["WATCH", key], b"OK")
    check(watcher, ["MULTI"], b"OK")
    queue(watcher, [["SET", marker, "must-not-exist"]])
    aborted_watch(watcher)
    check(other, ["EXISTS", marker], 0)

    check(watcher, ["WATCH", key], b"OK")
    check(other, ["SET", unrelated, "unrelated-mutation"], b"OK")
    assert execute(watcher, [["SET", marker, "committed"], ["GET", key]]) == [
        b"OK", b"same-connection-change",
    ]
    # EXEC (successful or aborted) and DISCARD clear old watches.
    check(other, ["SET", key, "after-exec"], b"OK")
    assert execute(watcher, [["GET", key]]) == [b"after-exec"]
    check(watcher, ["WATCH", key], b"OK")
    check(watcher, ["MULTI"], b"OK")
    queue(watcher, [["SET", marker, "discarded"]])
    check(watcher, ["DISCARD"], b"OK")
    check(other, ["SET", key, "after-discard"], b"OK")
    assert execute(watcher, [["GET", key], ["GET", marker]]) == [b"after-discard", b"committed"]
    check(watcher, ["WATCH", key], b"OK")
    check(watcher, ["UNWATCH"], b"OK")
    check(other, ["SET", key, "after-unwatch"], b"OK")
    assert execute(watcher, [["GET", key]]) == [b"after-unwatch"]
    passed("WATCH retains original tokens across repeated/extended WATCH and same-connection writes; unrelated mutations commit; EXEC/DISCARD/UNWATCH clear watches")


def watch_mutations(server):
    watcher, other = server.connect(), server.connect()
    string, items, members, scores, marker = [
        PREFIX + suffix for suffix in (b"same-string", b"same-list", b"same-set", b"same-zset", b"mutation-marker")
    ]
    check(other, ["SET", string, "same"], b"OK")
    check(other, ["RPUSH", items, "same"], 1)
    check(other, ["SADD", members, "same"], 1)
    check(other, ["ZADD", scores, 3, "same"], 1)
    for key, mutation, expected in (
        (string, ["SET", string, "same"], b"OK"),
        (items, ["LSET", items, 0, "same"], b"OK"),
    ):
        check(watcher, ["WATCH", key], b"OK")
        check(other, mutation, expected)
        check(watcher, ["MULTI"], b"OK")
        queue(watcher, [["SET", marker, "must-not-exist"]])
        aborted_watch(watcher)
        check(other, ["EXISTS", marker], 0)

    for key, mutation in (
        (members, ["SADD", members, "same"]),
        (scores, ["ZADD", scores, 3, "same"]),
    ):
        check(watcher, ["WATCH", key], b"OK")
        check(other, mutation, 0)
        assert execute(watcher, [["EXISTS", key]]) == [1]

    check(watcher, ["WATCH", string], b"OK")
    check(other, ["DEL", string], 1)
    check(other, ["SET", string, "same"], b"OK")
    check(watcher, ["MULTI"], b"OK")
    queue(watcher, [["SET", marker, "must-not-exist"]])
    aborted_watch(watcher)
    check(other, ["EXISTS", marker], 0)
    check(other, ["GET", string], b"same")
    passed("WATCH detects same-value SET/LSET and delete/recreate ABA, but no-op SADD/ZADD preserve validity")


def watch_expiration(server):
    watcher, other = server.connect(), server.connect()
    expiring, absent, marker = [
        PREFIX + suffix for suffix in (b"expiring-watch", b"absent-watch", b"expiry-marker")
    ]
    check(other, ["SET", expiring, "temporary", "PX", 1000], b"OK")
    check(watcher, ["WATCH", expiring], b"OK")
    # Read the deadline, not the key after expiry: EXEC must notice expiry even
    # without a GET from another client forcing lazy deletion first.
    deadline = other.command("PEXPIRETIME", expiring)
    assert isinstance(deadline, int) and deadline > current_ms(), deadline
    check(watcher, ["MULTI"], b"OK")
    queue(watcher, [["SET", marker, "must-not-exist"]])
    wait_until_ms(deadline + 25)
    aborted_watch(watcher)
    check(other, ["EXISTS", marker], 0)
    check(other, ["GET", expiring], None)

    # Watching the now-expired key and a never-created key records absence, not
    # an old expiration token that can spuriously invalidate this transaction.
    check(watcher, ["WATCH", expiring, absent], b"OK")
    check(other, ["SET", PREFIX + b"unrelated-expiry", "temporary", "PX", 50], b"OK")
    wait_until_ms(current_ms() + 100)
    assert execute(watcher, [["SET", marker, "committed"], ["EXISTS", expiring, absent]]) == [b"OK", 0]
    passed("expiration invalidates WATCH on existing keys without a triggering GET; watching absent/previously expired keys does not abort")


def blocking_in_exec(server):
    client, observer = server.connect(), server.connect()
    missing, marker, available = [
        PREFIX + suffix for suffix in (b"blocking-missing", b"blocking-marker", b"blocking-available")
    ]
    check(client, ["MULTI"], b"OK")
    queue(client, [
        ["BLPOP", missing, 0], ["BRPOP", missing, 60],
        ["BLMOVE", missing, available, "RIGHT", "LEFT", 0],
        ["BLMOVE", missing, available, "LEFT", "RIGHT", 60],
        ["SET", marker, "continued"],
    ])
    started = time.monotonic()
    client.socket.sendall(Client.frame(["EXEC"]))
    for expected in (b"*5\r\n", b"*-1\r\n", b"*-1\r\n", b"$-1\r\n", b"$-1\r\n"):
        actual = client.file.readline()
        assert actual == expected, ("EXEC blocking-pop reply", actual, expected)
    assert client.read() == b"OK"
    assert time.monotonic() - started < IO_TIMEOUT, "EXEC blocked on an empty list"
    check(observer, ["GET", marker], b"continued")
    reply = execute(client, [
        ["RPUSH", available, "left", "middle", "right"],
        ["BLPOP", available, 0], ["BRPOP", available, 60], ["LRANGE", available, 0, -1],
    ])
    assert reply == [3, [available, b"left"], [available, b"right"], [b"middle"]], reply
    destination = PREFIX + b"blocking-move-destination"
    reply = execute(client, [
        ["RPUSH", available, "left", "right"],
        ["BLMOVE", available, destination, "RIGHT", "LEFT", 0],
        ["BLMOVE", available, destination, "LEFT", "RIGHT", 60],
        ["LMOVE", destination, destination, "RIGHT", "LEFT"],
        ["LRANGE", destination, 0, -1],
    ])
    assert reply == [3, b"right", b"middle", b"middle", [b"middle", b"right"]], reply
    check(client, ["LPOP", available], b"left")
    passed("blocking list commands in EXEC return immediate values or exact null-array/null-bulk frames without blocking")


def commit_wakeups(server):
    control = server.connect()
    key = PREFIX + b"commit-wakeup"
    waiters = [server.connect() for _ in range(3)]
    for waiter, operation in zip(waiters, ("BLPOP", "BRPOP", "BLPOP")):
        begin_wait(waiter, [operation, key, 0], control)

    # A transient nonempty list is not visible to consumers outside EXEC.
    reply = execute(control, [["RPUSH", key, "transient"], ["LPOP", key]])
    assert reply == [1, b"transient"], reply
    for waiter in waiters:
        expect_quiet(waiter)
    check(control, ["EXISTS", key], 0)

    # FIFO wakeups must use the final state, not steal an intermediate item.
    reply = execute(control, [
        ["RPUSH", key, "discard-left", "a", "b", "c", "d", "discard-right"],
        ["LPOP", key], ["RPOP", key], ["LRANGE", key, 0, -1],
    ])
    assert reply == [6, b"discard-left", b"discard-right", [b"a", b"b", b"c", b"d"]], reply
    actual = [waiter.read() for waiter in waiters]
    assert actual == [[key, b"a"], [key, b"d"], [key, b"b"]], actual
    check(control, ["LRANGE", key, 0, -1], [b"c"])
    for waiter in waiters:
        check(waiter, ["PING"], b"PONG")
    passed("push-then-pop EXEC leaves outside consumers blocked; retained final list wakes mixed-end consumers FIFO only after commit")
    source, primed, final = [PREFIX + suffix for suffix in (
        b"exec-move-source", b"exec-move-primed", b"exec-move-final",
    )]
    mover, consumer = server.connect(), server.connect()
    begin_wait(mover, ["BLMOVE", primed, final, "RIGHT", "LEFT", 0], control)
    begin_wait(consumer, ["BLPOP", final, 0], control)
    reply = execute(control, [
        ["RPUSH", source, "job"],
        ["LMOVE", source, primed, "RIGHT", "LEFT"],
        ["LMOVE", primed, source, "LEFT", "RIGHT"],
    ])
    assert reply == [1, b"job", b"job"], reply
    expect_quiet(mover)
    expect_quiet(consumer)
    assert execute(control, [["LMOVE", source, primed, "RIGHT", "LEFT"]]) == [b"job"]
    assert mover.read() == b"job"
    assert consumer.read() == [final, b"job"]
    check(control, ["EXISTS", source, primed, final], 0)
    passed("LMOVE in EXEC wakes transitive BLMOVE/pop consumers only from the committed final list state")


def concurrent_tuples(server):
    keys = [PREFIX + b"atomic:%03d" % index for index in range(64)]
    control = server.connect()
    initial = b"initial"
    check(control, ["MSET", *[part for key in keys for part in (key, initial)]], b"OK")
    writers, readers, rounds = 2, 2, 40
    generations = {initial} | {
        b"writer:%d:generation:%d" % (writer, generation)
        for writer in range(writers) for generation in range(rounds)
    }
    clients = [server.connect() for _ in range(writers + readers)]
    barrier = threading.Barrier(writers + readers)
    stop, writers_done = threading.Event(), threading.Event()
    finish_lock = threading.Lock()
    finished = 0
    deadline = time.monotonic() + WORK_TIMEOUT

    def assert_tuple(reply, label):
        assert isinstance(reply, list) and len(reply) == len(keys), (label, reply)
        assert reply[0] in generations and all(value == reply[0] for value in reply), (
            label, "observed a torn or intermediate EXEC generation", reply,
        )

    def writer(index, client):
        nonlocal finished
        try:
            barrier.wait(timeout=IO_TIMEOUT)
            for generation in range(rounds):
                if stop.is_set():
                    return
                assert time.monotonic() < deadline, "EXEC writer exceeded watchdog"
                value = b"writer:%d:generation:%d" % (index, generation)
                order = keys if (index + generation) % 2 else keys[::-1]
                commands = [["SET", key, "uncommitted-intermediate"] for key in order]
                commands += [["SET", key, value] for key in order[::-1]]
                commands += [["MGET", *keys]]
                reply = execute(client, commands)
                assert isinstance(reply, list) and len(reply) == 2 * len(keys) + 1, reply
                assert reply[:-1] == [b"OK"] * (2 * len(keys)), reply
                assert reply[-1] == [value] * len(keys), reply
        finally:
            with finish_lock:
                finished += 1
                if finished == writers:
                    writers_done.set()

    def reader(index, client):
        barrier.wait(timeout=IO_TIMEOUT)
        reads = 0
        order = keys if index % 2 else keys[::-1]
        while not stop.is_set() and (not writers_done.is_set() or reads < 100):
            assert time.monotonic() < deadline, "tuple reader exceeded watchdog"
            if index == 0:
                reply = client.command("MGET", *order)
            else:
                reply = execute(client, [["GET", key] for key in order])
            assert_tuple(reply, "MGET" if index == 0 else "EXEC GET tuple")
            reads += 1
        return reads

    with concurrent.futures.ThreadPoolExecutor(max_workers=writers + readers) as pool:
        futures = [pool.submit(writer, index, clients[index]) for index in range(writers)]
        futures += [pool.submit(reader, index, clients[writers + index]) for index in range(readers)]
        try:
            results = [future.result(timeout=WORK_TIMEOUT + IO_TIMEOUT) for future in futures]
        finally:
            stop.set()
            barrier.abort()
    assert all(reads >= 100 for reads in results[writers:]), results
    assert_tuple(control.command("MGET", *keys), "final tuple")
    assert control.command("GET", keys[0]) != initial
    passed("concurrent cross-shard EXEC writers, MGET readers and EXEC GET readers see only whole committed tuples, including reversed key order")


def persistent_transactions(binary):
    with tempfile.TemporaryDirectory(prefix="ffkv-transactions-") as directory:
        with Server(binary, directory, durability="strict", extra_args=("--shards", "4")) as server:
            client, pending = server.connect(), server.connect()
            names = ("binary", "counter", "list", "before-error", "after-error", "deleted",
                     "discarded", "aborted", "pending", "unchanged")
            keys = {name: PREFIX + b"durable:" + name.encode() for name in names}
            payload = b"\x00\xff\r\n$3\r\nabc"
            check(client, ["MSET", keys["deleted"], "gone", keys["unchanged"], "original"], b"OK")
            reply = execute(client, [
                ["SET", keys["binary"], payload], ["INCRBY", keys["counter"], 7],
                ["RPUSH", keys["list"], "one", "two"], ["DEL", keys["deleted"]],
            ])
            assert reply == [b"OK", 7, 2, 1], reply
            reply = execute(client, [
                ["SET", keys["before-error"], "retained-before"],
                ["LPUSH", keys["before-error"], "wrong-type"],
                ["SET", keys["after-error"], "retained-after"],
                ["RPUSH", keys["list"], "three"], ["INCR", keys["counter"]],
            ])
            assert isinstance(reply, list) and len(reply) == 5, reply
            assert reply[0] == b"OK" and reply[2:] == [b"OK", 3, 8], reply
            error_category(reply[1], b"WRONGTYPE")
            check(client, ["MULTI"], b"OK")
            queue(client, [["SET", keys["discarded"], "must-not-replay"],
                           ["SET", keys["unchanged"], "discarded-overwrite"]])
            check(client, ["DISCARD"], b"OK")
            check(client, ["MULTI"], b"OK")
            queue(client, [["SET", keys["aborted"], "must-not-replay"]])
            check_error(client, ["SET", keys["aborted"]])
            check_error(client, ["EXEC"], b"EXECABORT")
            # Keep a live unexecuted transaction across the crash boundary.
            check(pending, ["MULTI"], b"OK")
            queue(pending, [["SET", keys["pending"], "must-not-replay"],
                            ["SET", keys["unchanged"], "queued-overwrite"]])
            server.stop(signal.SIGKILL)

            def verify():
                recovered = server.connect()
                check(recovered, ["MGET", keys["binary"], keys["counter"], keys["before-error"],
                                  keys["after-error"], keys["unchanged"]],
                      [payload, b"8", b"retained-before", b"retained-after", b"original"])
                check(recovered, ["LRANGE", keys["list"], 0, -1], [b"one", b"two", b"three"])
                for name in ("deleted", "discarded", "aborted", "pending"):
                    check(recovered, ["EXISTS", keys[name]], 0)
                assert execute(recovered, [["GET", keys["counter"]]]) == [b"8"]

            server.start()
            verify()
            server.stop()
            server.start()
            verify()
    passed("strict SIGKILL and orderly restart reproduce committed writes and runtime-error partial effects exactly, never queued/discarded/EXECABORT writes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    options = parser.parse_args()
    for shards in (1, 4):
        with Server(options.binary, extra_args=("--shards", str(shards))) as server:
            queue_and_errors(server)
            watch_tokens(server)
            watch_mutations(server)
            watch_expiration(server)
            blocking_in_exec(server)
            commit_wakeups(server)
            concurrent_tuples(server)
        passed(f"transaction scenarios complete with {shards} shard(s)")
    persistent_transactions(options.binary)
    passed("transaction smoke complete; owned processes, sockets, threads and temporary directories cleaned up")


if __name__ == "__main__":
    main()
