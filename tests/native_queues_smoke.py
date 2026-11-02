#!/usr/bin/env python3
"""Real FQ protocol/lifecycle/crash smoke; no Redis or test queue backend.

Usage: python3 tests/native_queues_smoke.py [--binary target/debug/ffkv]
Runs memory/hybrid storage, one/four shards, RESP2/RESP3, and SIGKILL recovery.
SIGKILL tests process-crash recovery, not device power-loss durability.
"""
import argparse
import concurrent.futures
from pathlib import Path
import signal
import tempfile
import time

from cli_smoke import Client
from durability_smoke import Server, current_ms, passed, wait_until_ms
from queue_smoke import IO_TIMEOUT, begin_wait, check, check_error


class NativeClient(Client):
    """Harness socket client with the RESP3 map/null types used by FQ."""

    def read(self):
        line = self.file.readline()
        assert line.endswith(b"\r\n"), ("truncated response", line)
        kind, data = line[:1], line[1:-2]
        if kind == b"+":
            return data
        if kind == b"-":
            return ("error", data)
        if kind == b":":
            return int(data)
        if kind == b"_":
            assert not data
            return None
        if kind == b"#":
            assert data in (b"t", b"f")
            return data == b"t"
        if kind == b"$":
            count = int(data)
            if count == -1:
                return None
            value = self.file.read(count)
            assert len(value) == count and self.file.read(2) == b"\r\n"
            return value
        if kind == b"*":
            count = int(data)
            return None if count == -1 else [self.read() for _ in range(count)]
        if kind == b"%":
            return {self.read(): self.read() for _ in range(int(data))}
        if kind == b"~":
            return [self.read() for _ in range(int(data))]
        if kind == b",":
            return float(data)
        raise AssertionError(("unexpected response", line))


def connect(server, resp3=True):
    client = NativeClient(server.address)
    client.socket.settimeout(IO_TIMEOUT)
    server.clients.append(client)
    if resp3:
        assert isinstance(client.command("HELLO", 3), dict)
    return client


def mapping(value):
    if isinstance(value, dict):
        return value
    assert isinstance(value, list) and len(value) % 2 == 0, value
    return dict(zip(value[::2], value[1::2]))


def enqueue(client, key, payload=b"body", *options):
    result = mapping(client.command("FQ.ENQUEUE", key, payload, *options))
    assert set(result) == {b"job_id", b"created"}, result
    assert isinstance(result[b"job_id"], bytes) and len(result[b"job_id"]) == 32, result
    assert result[b"created"] in (0, 1), result
    return result


def claim(client, key, *options):
    rows = client.command("FQ.CLAIM", key, *options)
    assert isinstance(rows, list), rows
    rows = [mapping(row) for row in rows]
    for row in rows:
        assert set(row) == {b"job_id", b"receipt", b"payload", b"attempts", b"deadline_ms"}, row
        assert isinstance(row[b"payload"], bytes) and isinstance(row[b"receipt"], bytes), row
        assert row[b"attempts"] > 0 and row[b"deadline_ms"] > 0, row
    return rows


def get(client, key, job_id):
    value = client.command("FQ.GET", key, job_id)
    return None if value is None else mapping(value)


def stats(client, key):
    value = mapping(client.command("FQ.STATS", key))
    value[b"totals"] = mapping(value[b"totals"])
    assert value[b"jobs"] == sum(value[state] for state in (b"ready", b"scheduled", b"running", b"dead")), value
    return value


def ack(client, key, row):
    check(client, ["FQ.ACK", key, row[b"receipt"]], b"OK")


def pages(client, key, state=None, count=2):
    cursor, found, visited = b"0", [], set()
    while True:
        args = ["FQ.JOBS", key, cursor, "COUNT", count]
        if state is not None:
            args += ["STATE", state]
        page = mapping(client.command(*args))
        assert set(page) == {b"cursor", b"jobs"}, page
        assert len(page[b"jobs"]) <= count, page
        for row in page[b"jobs"]:
            row = mapping(row)
            assert b"payload" not in row and b"receipt" not in row, row
            if state is not None:
                assert row[b"state"] == state, row
            found.append(row)
        cursor = page[b"cursor"]
        if cursor == b"0":
            break
        assert cursor not in visited, ("cursor loop", page)
        visited.add(cursor)
    ids = [row[b"job_id"] for row in found]
    assert ids == sorted(set(ids)), ids
    return found


def transaction(client, commands):
    check(client, ["MULTI"], b"OK")
    for command in commands:
        check(client, command, b"QUEUED")
    result = client.command("EXEC")
    assert isinstance(result, list) and len(result) == len(commands), result
    return result


def syntax_and_types(client):
    key = b"fq:syntax\x00\xff"
    commands = [
        ["FQ.ENQUEUE", key], ["FQ.CLAIM"], ["FQ.GET", key],
        ["FQ.ACK", key, "receipt", "extra"], ["FQ.STATS", key, "extra"],
        ["FQ.ENQUEUE", key, b"body", "DELAY", 0, "AT", 0],
        ["FQ.ENQUEUE", key, b"body", "PRIORITY", 2147483648],
        ["FQ.ENQUEUE", key, b"body", "DELAY", 9223372036854775807],
        ["FQ.ENQUEUE", key, b"body", "AT", -1],
        ["FQ.ENQUEUE", key, b"body", "IDEMPOTENCY", b"x" * 1025],
        ["FQ.NACK", key, "receipt", "ERROR", b"x" * 4097],
        ["FQ.JOBS", key, "bad-cursor"], ["FQ.JOBS", key, 0, "STATE", "finished"],
        ["FQ.CONFIG", key, "BACKOFF", 2, "MAXBACKOFF", 1],
        ["FQ.CONFIG", key, "IDEMPOTENCY", 0],
    ]
    for command in ("FQ.ENQUEUE", "FQ.CLAIM", "FQ.NACK", "FQ.RETRY", "FQ.CONFIG", "FQ.JOBS"):
        base = [command, key]
        if command in ("FQ.ENQUEUE", "FQ.NACK", "FQ.RETRY", "FQ.JOBS"):
            base.append(b"0")
        commands += [base + ["UNKNOWN", 1], base + ["COUNT"]]
    for base, option, values in [
        (["FQ.ENQUEUE", key, b"body"], "MAXATTEMPTS", [0, 1000001, -1, "01"]),
        (["FQ.CLAIM", key], "COUNT", [0, 1001, -1, "+1", "1.5"]),
        (["FQ.CLAIM", key], "VISIBILITY", [0, 86400001, -1]),
        (["FQ.NACK", key, "receipt"], "DELAY", [-1, 86400001]),
        (["FQ.RETRY", key, "id"], "DELAY", [-1, 86400001]),
        (["FQ.CONFIG", key], "CONCURRENCY", [-1, 1000001]),
        (["FQ.JOBS", key, 0], "COUNT", [0, 1001]),
    ]:
        commands.extend(base + [option, value] for value in values)
        commands.append(base + [option, 1, option.lower(), 1])
    for command in commands:
        check_error(client, command)
    check(client, ["EXISTS", key], 0)
    check(client, ["SET", key, "string"], b"OK")
    valid = [
        ["FQ.ENQUEUE", key, b"body"], ["FQ.CLAIM", key], ["FQ.ACK", key, "receipt"],
        ["FQ.NACK", key, "receipt"], ["FQ.EXTEND", key, "receipt", 1],
        ["FQ.RETRY", key, "id"], ["FQ.CANCEL", key, "id"], ["FQ.CONFIG", key],
        ["FQ.STATS", key], ["FQ.JOBS", key, 0], ["FQ.GET", key, "id"],
    ]
    for command in valid:
        check_error(client, command, b"WRONGTYPE")
    check(client, ["DEL", key], 1)
    enqueue(client, key)
    for command in (["GET", key], ["LPUSH", key, "x"], ["HSET", key, "f", "v"]):
        check_error(client, command, b"WRONGTYPE")
    check(client, ["DEL", key], 1)


def ordering_and_time(client):
    key = b"fq:ordering\x00\xff"
    payload = bytes(range(256)) * 40 + b"\r\n$4\r\n\x00\xff"
    low = enqueue(client, key, b"low", "PRIORITY", -2147483648)[b"job_id"]
    first = enqueue(client, key, payload, "priority", 7)[b"job_id"]
    second = enqueue(client, key, b"second", "PRIORITY", 7)[b"job_id"]
    due = current_ms() + 400
    high = enqueue(client, key, b"future", "PRIORITY", 2147483647, "AT", due)[b"job_id"]
    assert get(client, key, high)[b"eligible_at_ms"] == due
    rows = claim(client, key, "COUNT", 10)
    assert [row[b"job_id"] for row in rows] == [first, second, low], rows
    assert rows[0][b"payload"] == payload
    assert claim(client, key) == []
    for row in rows:
        ack(client, key, row)
    wait_until_ms(due)
    row, = claim(client, key)
    assert row[b"job_id"] == high
    ack(client, key, row)
    # DELAY uses the same frozen command timestamp as enqueue metadata.
    delayed = enqueue(client, key, b"delayed", "DELAY", 100)[b"job_id"]
    info = get(client, key, delayed)
    assert info[b"eligible_at_ms"] - info[b"enqueued_at_ms"] == 100, info
    assert claim(client, key) == []
    wait_until_ms(info[b"eligible_at_ms"])
    row, = claim(client, key)
    ack(client, key, row)
    # In one EXEC, DELAY 0 is due at exactly the frozen CLAIM timestamp.
    added, granted = transaction(client, [["FQ.ENQUEUE", key, b"boundary", "DELAY", 0], ["FQ.CLAIM", key]])
    row = mapping(granted[0])
    assert row[b"job_id"] == mapping(added)[b"job_id"]
    ack(client, key, row)
    started = time.monotonic()
    assert claim(client, key) == []
    assert time.monotonic() - started < 1, "empty CLAIM blocked"
    check(client, ["EXISTS", key], 1)
    totals = stats(client, key)[b"totals"]
    assert totals[b"enqueued"] == totals[b"claimed"] == totals[b"acked"] == 6, totals
    assert totals[b"queue_wait_count"] == totals[b"run_count"] == 6, totals
    assert totals[b"queue_wait_ms"] >= 0 and totals[b"run_ms"] >= 0, totals


def lifecycle(client):
    key = b"fq:lifecycle"
    policy = mapping(client.command("FQ.CONFIG", key, "CONCURRENCY", 1, "MAXATTEMPTS", 2,
                                    "BACKOFF", 20, "MAXBACKOFF", 40, "IDEMPOTENCY", 200))
    assert policy == {b"concurrency": 1, b"max_attempts": 2, b"backoff_ms": 20,
                      b"max_backoff_ms": 40, b"idempotency_ms": 200}, policy
    a = enqueue(client, key, b"a", "IDEMPOTENCY", b"dedupe\x00\xff")
    assert enqueue(client, key, b"different", "IDEMPOTENCY", b"dedupe\x00\xff") == {b"job_id": a[b"job_id"], b"created": 0}
    b = enqueue(client, key, b"b", "MAXATTEMPTS", 1)
    row, = claim(client, key, "COUNT", 100, "VISIBILITY", 200)
    assert row[b"job_id"] == a[b"job_id"] and row[b"attempts"] == 1
    assert claim(client, key) == []
    forged = row[b"receipt"][:-1] + (b"0" if row[b"receipt"][-1:] != b"0" else b"1")
    for receipt in (b"", b"forged\x00\xff", forged):
        for command, tail in (("FQ.ACK", []), ("FQ.NACK", []), ("FQ.EXTEND", [100])):
            check_error(client, [command, key, receipt, *tail])
    check_error(client, ["FQ.RETRY", key, a[b"job_id"]])
    old_deadline = row[b"deadline_ms"]
    new_deadline = client.command("FQ.EXTEND", key, row[b"receipt"], 500)
    assert new_deadline > old_deadline and get(client, key, a[b"job_id"])[b"deadline_ms"] == new_deadline
    wait_until_ms(old_deadline)
    assert get(client, key, a[b"job_id"])[b"state"] == b"running"
    assert claim(client, key) == []
    check(client, ["FQ.NACK", key, row[b"receipt"], "DELAY", 0, "ERROR", b"failure\x00\xff"], b"OK")
    check_error(client, ["FQ.ACK", key, row[b"receipt"]])
    again, = claim(client, key)
    assert again[b"job_id"] == a[b"job_id"] and again[b"attempts"] == 2
    assert again[b"receipt"] != row[b"receipt"]
    check(client, ["FQ.NACK", key, again[b"receipt"], "ERROR", b"dead\x00\xff"], b"OK")
    dead = get(client, key, a[b"job_id"])
    assert dead[b"state"] == b"dead" and dead[b"error"] == b"dead\x00\xff" and dead[b"payload"] == b"a", dead
    other, = claim(client, key, "VISIBILITY", 20)
    assert other[b"job_id"] == b[b"job_id"]
    wait_until_ms(other[b"deadline_ms"])
    for command, tail in (("FQ.ACK", []), ("FQ.NACK", []), ("FQ.EXTEND", [100])):
        check_error(client, [command, key, other[b"receipt"], *tail])
    assert get(client, key, b[b"job_id"])[b"state"] == b"dead"
    check(client, ["FQ.RETRY", key, a[b"job_id"], "DELAY", 100], b"OK")
    reset = get(client, key, a[b"job_id"])
    assert reset[b"attempts"] == 0 and reset[b"state"] == b"scheduled" and reset[b"error"] is None, reset
    assert claim(client, key) == []
    wait_until_ms(reset[b"eligible_at_ms"])
    redriven, = claim(client, key)
    assert redriven[b"attempts"] == 1 and redriven[b"receipt"] not in (row[b"receipt"], again[b"receipt"])
    check_error(client, ["FQ.ACK", key, again[b"receipt"]])
    ack(client, key, redriven)
    assert get(client, key, a[b"job_id"]) is None
    check(client, ["FQ.CANCEL", key, b[b"job_id"]], b"OK")
    assert stats(client, key)[b"jobs"] == 0
    # Explicit terminal-record retention independent of wall time above.
    terminal_idempotency(client)
    totals = stats(client, key)[b"totals"]
    assert totals[b"failed"] == 3 and totals[b"expired_leases"] == 1 and totals[b"retried"] == 2, totals
    assert totals[b"canceled"] == 1 and totals[b"acked"] == 1, totals


def terminal_idempotency(client):
    key = b"fq:idempotency"
    mapping(client.command("FQ.CONFIG", key, "IDEMPOTENCY", 300))
    for terminal in ("ack", "cancel"):
        token = terminal.encode() + b"\x00\xff"
        original = enqueue(client, key, b"original", "IDEMPOTENCY", token)
        if terminal == "ack":
            row, = claim(client, key)
            ack(client, key, row)
        else:
            check(client, ["FQ.CANCEL", key, original[b"job_id"]], b"OK")
        assert enqueue(client, key, b"replacement", "IDEMPOTENCY", token) == {b"job_id": original[b"job_id"], b"created": 0}
        wait_until_ms(current_ms() + 310)
        replacement = enqueue(client, key, b"new", "IDEMPOTENCY", token)
        assert replacement[b"created"] == 1 and replacement[b"job_id"] != original[b"job_id"]
        check(client, ["FQ.CANCEL", key, replacement[b"job_id"]], b"OK")
    wait_until_ms(current_ms() + 310)
    assert stats(client, key)[b"idempotency_records"] == 0


def retry_backoff_and_cancel(client):
    key = b"fq:backoff"
    mapping(client.command("FQ.CONFIG", key, "BACKOFF", 100, "MAXBACKOFF", 150, "MAXATTEMPTS", 4))
    job_id = enqueue(client, key)[b"job_id"]
    for delay in (100, 150, 150):
        row, = claim(client, key)
        result, clock = transaction(client, [
            ["FQ.NACK", key, row[b"receipt"]],
            ["FQ.ENQUEUE", key, b"clock", "DELAY", 60000],
        ])
        assert result == b"OK"
        clock_id = mapping(clock)[b"job_id"]
        timestamp = get(client, key, clock_id)[b"enqueued_at_ms"]
        info = get(client, key, job_id)
        assert info[b"state"] == b"scheduled" and info[b"eligible_at_ms"] - timestamp == delay, info
        check(client, ["FQ.CANCEL", key, clock_id], b"OK")
        wait_until_ms(info[b"eligible_at_ms"])
    row, = claim(client, key)
    check(client, ["FQ.CANCEL", key, job_id], b"OK")
    check_error(client, ["FQ.ACK", key, row[b"receipt"]])
    check_error(client, ["FQ.CANCEL", key, job_id])
    for options in ((), ("DELAY", 60000)):
        new_id = enqueue(client, key, b"cancel", *options)[b"job_id"]
        check(client, ["FQ.CANCEL", key, new_id], b"OK")
    assert stats(client, key)[b"jobs"] == 0


def cursor_and_concurrency(server, client):
    key = b"fq:pages"
    ids = [enqueue(client, key, str(i).encode(), *( ["DELAY", 60000] if i % 2 else [] ))[b"job_id"] for i in range(9)]
    assert [row[b"job_id"] for row in pages(client, key)] == sorted(ids)
    assert len(pages(client, key, b"ready")) == 5
    assert len(pages(client, key, b"scheduled")) == 4
    mapping(client.command("FQ.CONFIG", key, "CONCURRENCY", 2))
    workers = [connect(server) for _ in range(4)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(lambda worker: claim(worker, key, "COUNT", 100), workers))
    rows = [row for result in results for row in result]
    assert len(rows) == 2 and len({row[b"job_id"] for row in rows}) == 2, rows
    assert stats(client, key)[b"running"] == 2
    assert len(pages(client, key, b"running", count=1)) == 2
    assert pages(client, key, b"dead") == []
    check(client, ["FQ.CANCEL", key, rows[0][b"job_id"]], b"OK")
    next_row, = claim(client, key, "COUNT", 100)
    ack(client, key, next_row)
    ack(client, key, rows[1])
    mapping(client.command("FQ.CONFIG", key, "CONCURRENCY", 0))
    assert len(claim(client, key, "COUNT", 100)) == 2


def ttl_and_recreation(client):
    key = b"fq:ttl"
    enqueue(client, key)
    row, = claim(client, key)
    check(client, ["PEXPIRE", key, 60000], 1)
    before = client.command("PTTL", key)
    assert 0 < before <= 60000
    enqueue(client, key, b"second")
    assert 0 < client.command("PTTL", key) <= before
    check(client, ["PERSIST", key], 1)
    check(client, ["PTTL", key], -1)
    check(client, ["DEL", key], 1)
    enqueue(client, key, b"new epoch")
    replacement, = claim(client, key)
    assert replacement[b"receipt"] != row[b"receipt"]
    check_error(client, ["FQ.ACK", key, row[b"receipt"]])
    ack(client, key, replacement)
    check(client, ["EXISTS", key], 1)
    enqueue(client, key)
    expiring, = claim(client, key)
    check(client, ["PEXPIRE", key, 20], 1)
    wait_until_ms(current_ms() + 30)
    check(client, ["EXISTS", key], 0)
    enqueue(client, key)
    check_error(client, ["FQ.ACK", key, expiring[b"receipt"]])
    check(client, ["DEL", key], 1)


def policy_updates(client):
    key = b"fq:policy-updates"
    mapping(client.command("FQ.CONFIG", key, "MAXATTEMPTS", 3))
    original = enqueue(client, key)[b"job_id"]
    assert get(client, key, original)[b"max_attempts"] == 3
    policy = mapping(client.command("FQ.CONFIG", key, "MAXATTEMPTS", 1, "BACKOFF", 0, "MAXBACKOFF", 0))
    future = enqueue(client, key)[b"job_id"]
    assert get(client, key, original)[b"max_attempts"] == 3
    assert get(client, key, future)[b"max_attempts"] == 1
    check_error(client, ["FQ.CONFIG", key, "BACKOFF", 1])
    assert mapping(client.command("FQ.CONFIG", key)) == policy
    for job_id in (original, future):
        check(client, ["FQ.CANCEL", key, job_id], b"OK")
    assert mapping(client.command("FQ.CONFIG", key)) == policy
    check(client, ["DEL", key], 1)
    client.command("FQ.CONFIG", key)
    check(client, ["EXISTS", key], 0)


def protocol_shapes(server, client):
    key = b"fq:resp2"
    resp2 = connect(server, resp3=False)
    added = resp2.command("FQ.ENQUEUE", key, b"\x00\xff")
    assert isinstance(added, list), added
    job_id = mapping(added)[b"job_id"]
    assert isinstance(resp2.command("FQ.GET", key, job_id), list)
    assert isinstance(client.command("FQ.GET", key, job_id), dict)
    details = get(client, key, job_id)
    assert set(details) == {
        b"job_id", b"state", b"priority", b"sequence", b"enqueued_at_ms", b"eligible_at_ms",
        b"attempts", b"max_attempts", b"generation", b"deadline_ms", b"error", b"payload_bytes", b"payload",
    }, details
    assert details[b"payload_bytes"] == 2 and details[b"payload"] == b"\x00\xff"
    assert isinstance(client.command("FQ.CONFIG", key), dict)
    assert isinstance(client.command("FQ.STATS", key), dict)
    assert isinstance(client.command("FQ.JOBS", key, 0), dict)
    row, = claim(resp2, key)
    assert row[b"payload"] == b"\x00\xff"
    ack(resp2, key, row)
    check(resp2, ["FQ.GET", key, job_id], None)
    check(client, ["FQ.GET", key, job_id], None)
    # Descriptor registration/key routing is part of the actual CLI surface.
    for command in ("ENQUEUE", "CLAIM", "ACK", "NACK", "EXTEND", "RETRY", "CANCEL", "CONFIG", "STATS", "JOBS", "GET"):
        info = resp2.command("COMMAND", "INFO", "FQ." + command)
        assert isinstance(info, list) and len(info) == 1 and info[0] is not None, info
    check(resp2, ["COMMAND", "GETKEYS", "FQ.ENQUEUE", key, b"body", "IDEMPOTENCY", b"not-a-key"], [key])
    empty = enqueue(client, b"", b"", "IDEMPOTENCY", b"")
    row, = claim(client, b"")
    assert row[b"payload"] == b""
    ack(client, b"", row)
    assert enqueue(client, b"", b"not-empty", "IDEMPOTENCY", b"") == {b"job_id": empty[b"job_id"], b"created": 0}
    check(client, ["DEL", b""], 1)


def storage_args(mode, shards):
    args = ["--shards", str(shards), "--storage", mode]
    if mode == "hybrid":
        args += ["--inline-threshold", "1024", "--cache-bytes", "65536",
                 "--segment-bytes", "1048576", "--disk-budget", "0"]
    return tuple(args)


def recovery(server, mode, shards):
    client = connect(server)
    key = b"fq:recovery\x00\xff"
    policy = mapping(client.command("FQ.CONFIG", key, "CONCURRENCY", 4, "MAXATTEMPTS", 3,
                                    "BACKOFF", 0, "MAXBACKOFF", 0, "IDEMPOTENCY", 60000))
    payload = bytes(range(256)) * 48
    ready = enqueue(client, key, payload, "IDEMPOTENCY", b"live\x00\xff")[b"job_id"]
    live, = claim(client, key, "VISIBILITY", 60000)
    assert live[b"job_id"] == ready
    dead_id = enqueue(client, key, b"dead", "MAXATTEMPTS", 1)[b"job_id"]
    dead, = claim(client, key)
    check(client, ["FQ.NACK", key, dead[b"receipt"], "ERROR", b"persist\x00\xff"], b"OK")
    scheduled = enqueue(client, key, payload, "DELAY", 60000)[b"job_id"]
    terminal = enqueue(client, key, b"done", "IDEMPOTENCY", b"terminal")[b"job_id"]
    done, = claim(client, key)
    ack(client, key, done)
    expire_id = enqueue(client, key, b"expired offline")[b"job_id"]
    expire, = claim(client, key, "VISIBILITY", 250)
    ready_id = enqueue(client, key, b"ready")[b"job_id"]
    before = {job_id: get(client, key, job_id) for job_id in (ready, dead_id, scheduled, ready_id)}
    before_totals = stats(client, key)[b"totals"]
    ttl_key = b"fq:recovery:ttl"
    enqueue(client, ttl_key)
    check(client, ["PEXPIRE", ttl_key, 100], 1)
    blocked = server.connect()
    blocking_key = b"fq:recovery:blocking-list"
    begin_wait(blocked, ["BRPOP", blocking_key, 0], client)
    server.stop(signal.SIGKILL)
    wait_until_ms(expire[b"deadline_ms"] + 10)
    server.extra_args = storage_args(mode, 4 if shards == 1 else 1)
    server.start()
    client = connect(server)
    assert mapping(client.command("FQ.CONFIG", key)) == policy
    for job_id, expected in before.items():
        assert get(client, key, job_id) == expected, (job_id, expected, get(client, key, job_id))
    assert get(client, key, expire_id)[b"state"] == b"ready"
    after_totals = stats(client, key)[b"totals"]
    assert after_totals[b"expired_leases"] == before_totals[b"expired_leases"] + 1
    check_error(client, ["FQ.ACK", key, expire[b"receipt"]])
    check(client, ["EXISTS", ttl_key], 0)
    # The exact saved live receipt remains valid after process/shard restart.
    assert enqueue(client, key, b"not duplicated", "IDEMPOTENCY", b"live\x00\xff") == {b"job_id": ready, b"created": 0}
    assert enqueue(client, key, b"not duplicated", "IDEMPOTENCY", b"terminal") == {b"job_id": terminal, b"created": 0}
    ack(client, key, live)
    check_error(client, ["FQ.ACK", key, dead[b"receipt"]])
    redelivery = claim(client, key, "COUNT", 100, "VISIBILITY", 60000)
    assert {row[b"job_id"] for row in redelivery} == {expire_id, ready_id}, redelivery
    new_expire = next(row for row in redelivery if row[b"job_id"] == expire_id)
    assert new_expire[b"attempts"] == 2 and new_expire[b"receipt"] != expire[b"receipt"]
    check(client, ["LPUSH", blocking_key, "not-consumed-by-restored-waiter"], 1)
    check(client, ["LLEN", blocking_key], 1)
    assert claim(client, key) == []
    snapshot = {row[b"job_id"]: get(client, key, row[b"job_id"]) for row in redelivery}
    server.stop(signal.SIGKILL)
    server.extra_args = storage_args(mode, shards)
    server.start()
    client = connect(server)
    for row in redelivery:
        assert get(client, key, row[b"job_id"]) == snapshot[row[b"job_id"]]
        ack(client, key, row)
    check(client, ["FQ.RETRY", key, dead_id], b"OK")
    retried, = claim(client, key)
    assert retried[b"job_id"] == dead_id and retried[b"attempts"] == 1 and retried[b"payload"] == b"dead"
    ack(client, key, retried)
    assert get(client, key, scheduled)[b"payload"] == payload
    check(client, ["FQ.CANCEL", key, scheduled], b"OK")
    assert stats(client, key)[b"jobs"] == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    options = parser.parse_args()
    binary = str(Path(options.binary).resolve())
    for mode in ("memory", "hybrid"):
        for shards in (1, 4):
            with tempfile.TemporaryDirectory(prefix=f"ffkv-native-{mode}-{shards}-") as directory:
                with Server(binary, Path(directory), durability="strict", extra_args=storage_args(mode, shards)) as server:
                    client = connect(server)
                    syntax_and_types(client)
                    ordering_and_time(client)
                    lifecycle(client)
                    retry_backoff_and_cancel(client)
                    cursor_and_concurrency(server, client)
                    ttl_and_recreation(client)
                    policy_updates(client)
                    protocol_shapes(server, client)
                    recovery(server, mode, shards)
            passed(f"native queues: {mode}, {shards} shards; binary RESP2/3, lifecycle, cursor/concurrency, SIGKILL/reopen")


if __name__ == "__main__":
    main()
