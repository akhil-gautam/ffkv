#!/usr/bin/env python3
"""Exercise a real ffkv process; optionally compare string semantics to Redis.

Usage: python3 tests/cli_smoke.py [--binary target/debug/ffkv]
       python3 tests/cli_smoke.py --reference 127.0.0.1:16380
The reference must be disposable: this script mutates keys prefixed ffkv:test:.
"""
import argparse
import concurrent.futures
import random
import socket
import subprocess
import time


class Client:
    def __init__(self, address):
        self.socket = socket.create_connection(address, timeout=5)
        self.file = self.socket.makefile("rb")

    def close(self):
        self.file.close()
        self.socket.close()

    @staticmethod
    def frame(args):
        args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        return b"*%d\r\n" % len(args) + b"".join(
            b"$%d\r\n" % len(a) + a + b"\r\n" for a in args
        )

    def read(self):
        line = self.file.readline()
        if not line.endswith(b"\r\n"):
            raise AssertionError(f"truncated response: {line!r}")
        kind, data = line[:1], line[1:-2]
        if kind == b"+":
            return data
        if kind == b"-":
            return ("error", data)
        if kind == b":":
            return int(data)
        if kind == b"$":
            n = int(data)
            if n == -1:
                return None
            value = self.file.read(n)
            assert len(value) == n and self.file.read(2) == b"\r\n"
            return value
        if kind == b"*":
            n = int(data)
            return None if n == -1 else [self.read() for _ in range(n)]
        raise AssertionError(f"unexpected response: {line!r}")

    def command(self, *args):
        self.socket.sendall(self.frame(args))
        return self.read()


def exercise(address, reference):
    client = Client(address)
    other = Client(reference) if reference else None
    count = 0

    def check(args, expected):
        nonlocal count
        actual = client.command(*args)
        assert actual == expected, (args, actual, expected)
        count += 1

    try:
        check(["PING"], b"PONG")
        key = b"ffkv:test:binary\x00\xff\r\n"
        value = b"\x00\xff\r\n$3\r\nabc\x00"
        check(["SET", key, value], b"OK")
        check(["GET", key], value)
        check(["SET", b"", b""], b"OK")
        check(["GET", b""], b"")
        check(["DEL", key, key], 1)
        check(["GET", key], None)

        # Fragmented frames followed by a pipeline: no bytes may be consumed early.
        frame = Client.frame(["SET", key, value])
        for byte in frame:
            client.socket.sendall(bytes([byte]))
        client.socket.sendall(Client.frame(["GET", key]) + Client.frame(["PING"]))
        assert [client.read(), client.read(), client.read()] == [b"OK", value, b"PONG"]
        print("PASS binary keys/values, empty values, fragmented framing and pipelining")

        k = b"ffkv:test:ttl"
        check(["SET", k, "10", "PX", 5000], b"OK")
        check(["SET", k, "11", "NX"], None)
        ttl = client.command("PTTL", k)
        assert 0 < ttl <= 5000, ttl
        check(["INCR", k], 11)
        assert 0 < client.command("PTTL", k) <= ttl
        check(["SET", k, "12", "GET", "KEEPTTL"], b"11")
        assert 0 < client.command("PTTL", k) <= ttl
        check(["PEXPIRE", k, 10, "GT"], 0)
        check(["PEXPIRE", k, 10000, "GT"], 1)
        check(["PERSIST", k], 1)
        check(["PTTL", k], -1)
        check(["PEXPIRE", k, 0], 1)
        check(["PTTL", k], -2)
        check(["SET", k, "temporary", "PX", 20], b"OK")
        deadline = time.monotonic() + 2
        while client.command("GET", k) is not None:
            assert time.monotonic() < deadline, "key did not expire"
            time.sleep(0.005)
        check(["SET", k, "old", "PX", 20], b"OK")
        check(["SET", k, "new"], b"OK")
        time.sleep(0.04)
        check(["GET", k], b"new")
        print("PASS TTL preservation, conditional expiry, immediate expiry and overwrite safety")

        k = b"ffkv:test:overflow"
        check(["SET", k, "9223372036854775807"], b"OK")
        assert client.command("INCR", k)[0] == "error"
        check(["GET", k], b"9223372036854775807")
        check(["SET", k, "-9223372036854775808"], b"OK")
        assert client.command("DECR", k)[0] == "error"
        check(["GET", k], b"-9223372036854775808")
        check(["SET", k, "01"], b"OK")
        assert client.command("INCR", k)[0] == "error"
        check(["GET", k], b"01")
        print("PASS integer overflow/invalid integer errors leave values unchanged")

        counter = b"ffkv:test:counter"
        check(["SET", counter, "0"], b"OK")
        def increment(_):
            worker = Client(address)
            try:
                return [worker.command("INCR", counter) for _ in range(100)]
            finally:
                worker.close()
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(increment, range(8)))
        assert sorted(x for group in results for x in group) == list(range(1, 801))
        check(["GET", counter], b"800")
        print("PASS concurrent clients: 800 atomic increments, no lost updates")

        for malformed in [b"*1\r\n$8388609\r\n", b"*1025\r\n", b"*1\r\n$1\r\nxZZ"]:
            broken = Client(address)
            try:
                broken.socket.sendall(malformed)
                assert broken.read()[0] == "error"
                assert broken.file.read(1) == b"", "protocol error must close connection"
            finally:
                broken.close()
        check(["PING"], b"PONG")
        print("PASS malformed/oversized frames close only the offending connection")

        if other:
            rng = random.Random(4217)
            keys = [b"ffkv:test:random:%d" % i for i in range(12)]
            for c in (client, other):
                c.command("DEL", *keys)
            for _ in range(1200):
                k = rng.choice(keys)
                op = rng.randrange(13)
                args = [
                    ["SET", k, rng.choice([b"0", b"42", b"-9", b"01", b"\x00\xff", b""])],
                    ["SET", k, "7", "NX", "GET"],
                    ["SET", k, "8", "XX", "GET"],
                    ["GET", k], ["DEL", k, k], ["EXISTS", k, k],
                    ["INCR", k], ["INCRBY", k, rng.randint(-50, 50)],
                    ["DECRBY", k, rng.randint(-50, 50)],
                    ["MGET", k, rng.choice(keys)],
                    ["MSET", k, "9", rng.choice(keys), "10"],
                    ["PEXPIREAT", k, 1], ["TYPE", k],
                ][op]
                actual, expected = client.command(*args), other.command(*args)
                assert actual == expected, (args, actual, expected)
            print("PASS 1200 seeded differential operations against reference Redis")
        print(f"PASS {count} explicit command assertions; ffkv CLI smoke complete")
    finally:
        client.close()
        if other:
            other.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--reference", help="Disposable Redis host:port; writes test keys")
    options = parser.parse_args()
    process = subprocess.Popen(
        [options.binary, "--bind", "127.0.0.1:0"], stderr=subprocess.PIPE, text=True
    )
    try:
        line = process.stderr.readline()
        assert "listening on " in line, line
        address = line.split("listening on ", 1)[1].split()[0]
        host, port = address.rsplit(":", 1)
        reference = None
        if options.reference:
            rh, rp = options.reference.rsplit(":", 1)
            reference = (rh, int(rp))
        exercise((host, int(port)), reference)
    finally:
        if process.poll() is None:
            process.send_signal(__import__("signal").SIGINT)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        if process.stderr:
            process.stderr.close()
    assert process.returncode == 0, process.returncode


if __name__ == "__main__":
    main()
