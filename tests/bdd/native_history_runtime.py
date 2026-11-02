"""Owned native consumers for source-time and chained-history regressions."""
import os
import signal
import socket
import time

from replication_runtime import ReplicationFixture, ReplicationProxy, Wire, encode, eventually, info


def fields(reply):
    if isinstance(reply, dict):
        return reply
    assert isinstance(reply, list) and len(reply) % 2 == 0, reply
    return dict(zip(reply[::2], reply[1::2]))


def follow(source, follower):
    assert follower.execute(["REPLICAOF", *source.address]) == "OK"
    eventually(lambda: info(follower), lambda value: value.get("master_link_status") == "up", "owned native follower did not synchronize")


def queue_source_time(root):
    pair = ReplicationFixture(root, synchronized=True)
    job = fields(pair.master.execute(["FQ.ENQUEUE", "source-clock-q", "body", "DELAY", 1500]))
    assert pair.master.execute(["WAIT", 1, 2000]) == 1
    sessions = len(pair.proxy.captures())
    os.kill(pair.master.process.pid, signal.SIGSTOP)
    try:
        time.sleep(1.7)
        stats = fields(pair.replica.execute(["FQ.STATS", "source-clock-q"]))
        assert (stats["scheduled"], stats["ready"]) == (1, 0), stats
        held = fields(pair.replica.execute(["FQ.GET", "source-clock-q", job["job_id"]]))
        assert held["state"] == "scheduled" and held["payload"] == "body", held
    finally:
        os.kill(pair.master.process.pid, signal.SIGCONT)
    return pair, job, sessions


def assert_queue_source_time(result):
    pair, job, sessions = result
    eventually(lambda: fields(pair.replica.execute(["FQ.GET", "source-clock-q", job["job_id"]])),
               lambda value: value.get("state") == "ready", "source promotion did not replay")
    assert pair.master.execute(["WAIT", 1, 2000]) == 1
    assert len(pair.proxy.captures()) == sessions, "follower-local queue timing forced a replacement synchronization"


def chained_elapsed_payload(root):
    pair = ReplicationFixture(root, synchronized=True)
    assert pair.master.execute(["RPUSH", "elapsed-chain-list", "retained"]) == 1
    assert pair.master.execute(["PEXPIRE", "elapsed-chain-list", 1500]) == 1
    assert pair.master.execute(["WAIT", 1, 2000]) == 1
    pair.proxy.change(paused=True)
    try:
        assert pair.master.execute(["PERSIST", "elapsed-chain-list"]) == 1
        time.sleep(1.7)
        assert pair.replica.execute(["LRANGE", "elapsed-chain-list", 0, -1]) == []
        downstream = root.add_node("elapsed-downstream", extra_args=["--native-replication-allow-loopback"])
        follow(pair.replica, downstream)
    finally:
        pair.proxy.change(paused=False)
    return pair, downstream


def assert_chained_elapsed_payload(result):
    pair, downstream = result
    for node in (pair.replica, downstream):
        eventually(lambda: node.execute(["LRANGE", "elapsed-chain-list", 0, -1]),
                   lambda value: value == ["retained"], "chained source PERSIST lost elapsed snapshot payload")
        assert node.execute(["PTTL", "elapsed-chain-list"]) == -1


def chained_history_replacement(root):
    options = ["--native-replication-allow-loopback"]
    primary = root.add_node("history-primary", extra_args=options)
    middle = root.add_node("history-middle", extra_args=options)
    downstream = root.add_node("history-downstream", extra_args=options)
    replacement = root.add_node("history-replacement", extra_args=options)
    assert primary.execute(["SET", "origin", "old"]) == "OK"
    assert replacement.execute(["SET", "origin", "new"]) == "OK"
    follow(primary, middle)
    proxy = ReplicationProxy(middle.address)
    root.cleanups.append(proxy.close)
    assert downstream.execute(["REPLICAOF", *proxy.address]) == "OK"
    eventually(lambda: downstream.execute(["GET", "origin"]), lambda value: value == "old", "old chained image missing")
    os.kill(primary.process.pid, signal.SIGSTOP)
    try:
        # The stopped source cannot answer INFO; sample its drained live follower.
        time.sleep(0.05)
        offset = int(info(middle)["master_repl_offset"])
        for node in (middle, downstream):
            eventually(lambda: info(node), lambda value: int(value["slave_repl_offset"]) == offset, "old chain did not reach frozen source offset")
        # Real native downstream heartbeats generate the exact GETACK byte history;
        # no fixture-invented offsets, effects, or snapshots are installed.
        with socket.create_connection(replacement.address, timeout=10) as observer:
            wire = Wire(observer)
            state = info(replacement)
            observer.sendall(encode(["PSYNC", state["master_replid"], int(state["master_repl_offset"]) + 1]))
            assert wire.line(time.monotonic() + 10).startswith(b"+CONTINUE")
            eventually(lambda: int(info(replacement)["master_repl_offset"]), lambda value: value >= offset, "replacement native heartbeat alignment")
        assert int(info(replacement)["master_repl_offset"]) == offset, "same-offset regression requires an exact byte boundary"
        assert info(replacement)["master_replid"] != info(middle)["master_replid"]
        follow(replacement, middle)
        assert middle.execute(["GET", "origin"]) == "new"
        assert replacement.execute(["SET", "replacement-tail", "delivered"]) == "OK"
        return downstream, proxy
    finally:
        os.kill(primary.process.pid, signal.SIGCONT)


def assert_chained_history_replacement(result):
    downstream, proxy = result
    eventually(lambda: downstream.execute(["GET", "replacement-tail"]), lambda value: value == "delivered", "replacement tail did not reach chained consumer")
    assert downstream.execute(["GET", "origin"]) == "new", "new history tail was applied to the obsolete chained image"
    assert len(proxy.captures()) >= 2, "authoritative image replacement did not fence old downstream transport"


def chained_promotion_continue(root):
    pair = ReplicationFixture(root, synchronized=True)
    downstream = root.add_node("promotion-downstream", extra_args=["--native-replication-allow-loopback"])
    proxy = ReplicationProxy(pair.replica.address)
    root.cleanups.append(proxy.close)
    assert downstream.execute(["REPLICAOF", *proxy.address]) == "OK"
    eventually(lambda: info(downstream), lambda value: value.get("master_link_status") == "up", "promotion chain did not synchronize")
    assert pair.master.execute(["SET", "promotion-base", "retained"]) == "OK"
    eventually(lambda: downstream.execute(["GET", "promotion-base"]), lambda value: value == "retained", "promotion base did not reach consumer")
    proxy.change(enabled=False)
    eventually(lambda: info(downstream), lambda value: value.get("master_link_status") == "down", "downstream did not disconnect before promotion")
    capture_index = len(proxy.captures())
    assert pair.replica.execute(["REPLICAOF", "NO", "ONE"]) == "OK"
    assert pair.replica.execute(["SET", "promotion-tail", "continued"]) == "OK"
    proxy.change(enabled=True)
    return downstream, proxy, capture_index


def assert_chained_promotion_continue(result):
    from replication_runtime import sync_capture
    downstream, proxy, capture_index = result
    eventually(lambda: downstream.execute(["GET", "promotion-tail"]), lambda value: value == "continued", "promoted history did not continue")
    assert downstream.execute(["GET", "promotion-base"]) == "retained"
    captures = [sync_capture(request, response) for request, response, _ in proxy.captures()[capture_index:]]
    assert any(capture is not None and capture[1] == "partial" for capture in captures), captures
