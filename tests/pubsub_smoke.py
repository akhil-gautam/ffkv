#!/usr/bin/env python3
"""Exercise owned-process RESP2/3 Pub/Sub, Lua/EXEC publication and cleanup.

Run after building: python3 tests/pubsub_smoke.py --binary target/debug/ffkv
"""
import argparse
import socket
import tempfile
import time
from pathlib import Path

from cli_smoke import Client
from durability_smoke import Server


class Subscriber(Client):
    def read(self):
        marker = self.file.peek(1)[:1]
        if marker in (b">", b"%"):
            line = self.file.readline()
            length = int(line[1:-2])
            if marker == b">":
                return ("push", [self.read() for _ in range(length)])
            entries = [(self.read(), self.read()) for _ in range(length)]
            return dict(entries)
        if marker == b"_":
            assert self.file.readline() == b"_\r\n"
            return None
        return super().read()


def check(client, args, expected):
    actual = client.command(*args)
    assert actual == expected, (args, actual, expected)


def exercise(server):
    publisher = server.connect()
    resp2 = Subscriber(server.address)
    resp3 = Subscriber(server.address)
    server.clients.extend([resp2, resp3])
    channel = b"binary:\x00\xff\r\n"
    payload = b"\x00\xff\r\n$5\r\nhello"
    hello = resp3.command("HELLO", 3)
    assert hello[b"proto"] == 3, hello
    check(resp2, ["SUBSCRIBE", channel], [b"subscribe", channel, 1])
    check(resp3, ["SUBSCRIBE", channel], ("push", [b"subscribe", channel, 1]))
    check(resp3, ["PSUBSCRIBE", b"binary:*"], ("push", [b"psubscribe", b"binary:*", 2]))
    check(publisher, ["PUBSUB", "NUMSUB", channel], [channel, 2])
    check(publisher, ["PUBSUB", "NUMPAT"], 1)
    check(publisher, ["PUBSUB", "CHANNELS", b"binary:*"], [channel])
    check(publisher, ["PUBLISH", channel, payload], 3)
    assert resp2.read() == [b"message", channel, payload]
    assert resp3.read() == ("push", [b"message", channel, payload])
    assert resp3.read() == ("push", [b"pmessage", b"binary:*", channel, payload])
    denied = resp2.command("GET", "not-allowed-in-resp2-subscribed-mode")
    assert denied[0] == "error" and b"subscribed mode" in denied[1], denied
    check(resp2, ["PING", payload], [b"pong", payload])
    check(resp3, ["PING"], b"PONG")
    check(publisher, ["EVAL", "redis.call('SET',KEYS[1],ARGV[2]); return redis.call('PUBLISH',ARGV[1],ARGV[2])", 1, "pubsub:atomic", channel, payload], 3)
    assert resp2.read() == [b"message", channel, payload]
    check(publisher, ["GET", "pubsub:atomic"], payload)
    assert resp3.read() == ("push", [b"message", channel, payload])
    assert resp3.read() == ("push", [b"pmessage", b"binary:*", channel, payload])
    check(publisher, ["MULTI"], b"OK")
    check(publisher, ["PUBLISH", channel, b"exec"], b"QUEUED")
    check(publisher, ["EXEC"], [3])
    assert resp2.read() == [b"message", channel, b"exec"]
    assert resp3.read() == ("push", [b"message", channel, b"exec"])
    assert resp3.read() == ("push", [b"pmessage", b"binary:*", channel, b"exec"])
    failed = publisher.command("EVAL", "redis.call('PUBLISH',ARGV[1],'before-error'); return redis.call('NOT_A_COMMAND')", 0, channel)
    assert failed[0] == "error", failed
    assert resp2.read() == [b"message", channel, b"before-error"]
    assert resp3.read() == ("push", [b"message", channel, b"before-error"])
    assert resp3.read() == ("push", [b"pmessage", b"binary:*", channel, b"before-error"])
    check(resp2, ["UNSUBSCRIBE"], [b"unsubscribe", channel, 0])
    check(resp2, ["UNSUBSCRIBE"], [b"unsubscribe", None, 0])
    check(resp2, ["GET", "pubsub:atomic"], payload)
    check(resp3, ["UNSUBSCRIBE", channel], ("push", [b"unsubscribe", channel, 1]))
    check(resp3, ["PUNSUBSCRIBE"], ("push", [b"punsubscribe", b"binary:*", 0]))
    check(publisher, ["PUBLISH", channel, b"unobserved"], 0)
    check(publisher, ["MULTI"], b"OK")
    invalid = publisher.command("SUBSCRIBE", channel)
    assert invalid[0] == "error", invalid
    aborted = publisher.command("EXEC")
    assert aborted[0] == "error" and aborted[1].startswith(b"EXECABORT"), aborted
    forbidden = publisher.command("EVAL", "return redis.call('SUBSCRIBE',ARGV[1])", 0, channel)
    assert forbidden[0] == "error", forbidden
    resp2.close()
    resp3.close()
    print("PASS RESP2 arrays/restrictions, RESP3 pushes, binary messages, real counts, Lua/EXEC and unsubscribe transitions", flush=True)


def blocked_resp3_delivery(server):
    publisher = server.connect()
    subscriber = Subscriber(server.address)
    server.clients.append(subscriber)
    subscriber.command("HELLO", 3)
    check(subscriber, ["SUBSCRIBE", b"while-blocked"], ("push", [b"subscribe", b"while-blocked", 1]))
    subscriber.socket.sendall(subscriber.frame(["BLMOVE", "pubsub:absent", "pubsub:destination", "RIGHT", "LEFT", 0]))
    check(publisher, ["PUBLISH", b"while-blocked", b"delivered-before-move"], 1)
    subscriber.socket.settimeout(2)
    assert subscriber.read() == ("push", [b"message", b"while-blocked", b"delivered-before-move"])
    check(publisher, ["LPUSH", "pubsub:absent", b"moved"], 1)
    assert subscriber.read() == b"moved"
    check(publisher, ["LRANGE", "pubsub:destination", 0, -1], [b"moved"])
    check(subscriber, ["UNSUBSCRIBE"], ("push", [b"unsubscribe", b"while-blocked", 0]))
    subscriber.close()
    print("PASS RESP3 pushes continue while BLMOVE waits, followed by the move reply", flush=True)


def matching_isolation(server):
    publisher = server.connect()
    subscriber = Subscriber(server.address)
    server.clients.append(subscriber)
    check(subscriber, ["PSUBSCRIBE", b"private:*"], [b"psubscribe", b"private:*", 1])
    rejected = publisher.command("PUBLISH", b"pub:" + b"x" * (1024 * 1024), b"")
    assert rejected[0] == "error" and b"pattern work limit" in rejected[1], rejected
    check(publisher, ["PUBLISH", b"private:ok", b"still-connected"], 1)
    assert subscriber.read() == [b"pmessage", b"private:*", b"private:ok", b"still-connected"]
    check(subscriber, ["PUNSUBSCRIBE"], [b"punsubscribe", b"private:*", 0])
    subscriber.close()
    print("PASS publisher matching-work rejection does not disconnect other subscribers", flush=True)


def slow_reader(server):
    publisher = server.connect()
    slow = Subscriber(server.address)
    server.clients.append(slow)
    slow.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    check(slow, ["SUBSCRIBE", b"slow"], [b"subscribe", b"slow", 1])
    payload = b"x" * (6 * 1024 * 1024)
    disconnected = False
    for _ in range(16):
        receivers = publisher.command("PUBLISH", b"slow", payload)
        assert receivers in (0, 1), receivers
        if receivers == 0:
            disconnected = True
            break
    assert disconnected, "slow subscriber was never removed on byte overflow"
    check(publisher, ["PUBSUB", "NUMSUB", b"slow"], [b"slow", 0])
    slow.close()
    fresh = Subscriber(server.address)
    server.clients.append(fresh)
    check(fresh, ["SUBSCRIBE", b"cleanup"], [b"subscribe", b"cleanup", 1])
    fresh.close()
    deadline = time.monotonic() + 3
    while publisher.command("PUBSUB", "NUMSUB", b"cleanup") != [b"cleanup", 0]:
        assert time.monotonic() < deadline, "disconnect retained channel registration"
        time.sleep(0.01)
    print("PASS slow-reader byte overflow disconnect and EOF registration cleanup", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="target/debug/ffkv")
    options = parser.parse_args()
    binary = str(Path(options.binary).resolve())
    with tempfile.TemporaryDirectory(prefix="ffkv-pubsub-") as directory:
        with Server(binary, directory, "strict") as server:
            exercise(server)
            blocked_resp3_delivery(server)
            matching_isolation(server)
            slow_reader(server)
        with Server(binary, directory, "strict") as server:
            check(server.connect(), ["PUBSUB", "CHANNELS"], [])
            check(server.connect(), ["PUBSUB", "NUMPAT"], 0)
            check(server.connect(), ["PUBLISH", b"binary:\x00\xff\r\n", b"after-restart"], 0)
    print("PASS persistent restart has no restored subscriptions or publication replay", flush=True)


if __name__ == "__main__":
    main()
