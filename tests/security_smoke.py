#!/usr/bin/env python3
"""Real RESP/TLS/ACL integration. Requires openssl, not Redis or a client package.

Usage: python3 tests/security_smoke.py --binary target/debug/ffkv
Owns every process, certificate, ACL and data directory. Never disables certificate
verification and never patches a client library. No build is performed here.
"""
import argparse
import contextlib
import os
from pathlib import Path
import signal
import socket
import ssl
import subprocess
import tempfile

from cli_smoke import Client
from queue_smoke import startup_address


class WireClient(Client):
    def __init__(self, address, context=None, hostname="localhost"):
        raw = socket.create_connection(address, timeout=7)
        try:
            self.socket = context.wrap_socket(raw, server_hostname=hostname) if context else raw
        except BaseException:
            raw.close()
            raise
        self.socket.settimeout(7)
        self.file = self.socket.makefile("rb")
        self.last_kind = None

    def read(self):
        return self._read(True)

    def _read(self, top=False):
        line = self.file.readline()
        assert line.endswith(b"\r\n"), ("truncated response", line)
        kind, data = line[:1], line[1:-2]
        if top:
            self.last_kind = kind
        if kind == b"+":
            return data
        if kind == b"-":
            return ("error", data)
        if kind == b":":
            return int(data)
        if kind == b"_":
            return None
        if kind == b"$":
            size = int(data)
            if size == -1:
                return None
            value = self.file.read(size)
            assert len(value) == size and self.file.read(2) == b"\r\n"
            return value
        if kind == b"*":
            size = int(data)
            return None if size == -1 else [self._read() for _ in range(size)]
        if kind == b"%":
            return {self._read(): self._read() for _ in range(int(data))}
        raise AssertionError(("unexpected RESP type", line))


class SecurityServer:
    def __init__(self, binary, *flags):
        self.binary, self.flags = binary, flags
        self.clients = []
        self.process = None

    def __enter__(self):
        self.process = subprocess.Popen(
            [self.binary, "--bind", "127.0.0.1:0", *map(str, self.flags)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
        )
        try:
            self.address = startup_address(self.process)
        except BaseException as exception:
            self.__exit__(type(exception), exception, exception.__traceback__)
            raise
        return self

    def connect(self, context=None, hostname="localhost"):
        client = WireClient(self.address, context, hostname)
        self.clients.append(client)
        return client

    def __exit__(self, exc_type, exc, traceback):
        for client in self.clients:
            with contextlib.suppress(OSError):
                client.close()
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
        try:
            _, diagnostics = self.process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            _, diagnostics = self.process.communicate(timeout=5)
            raise AssertionError("security server did not shut down") from None
        assert b"root-secret" not in diagnostics and b"scoped-secret" not in diagnostics, diagnostics
        if exc_type is None:
            assert self.process.returncode == 0, (self.process.returncode, diagnostics)


def equal(client, args, expected):
    actual = client.command(*args)
    assert actual == expected, (args[0], actual, expected)
    return actual


def failure(client, args, category=b"ERR"):
    reply = client.command(*args)
    assert isinstance(reply, tuple) and reply[0] == "error", (args[0], reply)
    assert reply[1].split(b" ", 1)[0] == category, (args[0], reply, category)
    return reply


def hello_map(reply):
    return reply if isinstance(reply, dict) else dict(zip(reply[::2], reply[1::2]))


def has_permission_error(reply):
    if isinstance(reply, tuple):
        return reply[0] == "error" and b"NOPERM" in reply[1]
    return isinstance(reply, list) and any(has_permission_error(value) for value in reply)


def acl_scenario(server, context):
    unauth = server.connect(context)
    for args in (["GET", "x"], ["INFO"], ["CLIENT", "ID"], ["MULTI"], ["WATCH", "x"], ["EVAL", "return 1", 0]):
        failure(unauth, args, b"NOAUTH")
    replies = [failure(unauth, ["AUTH", name, "wrong"], b"WRONGPASS") for name in ("admin", "missing", "default")]
    assert replies[0] == replies[1] == replies[2], replies
    failure(unauth, ["AUTH", b"u" * 257, "wrong"], b"WRONGPASS")
    failure(unauth, ["AUTH", "admin", b"p" * 1025], b"WRONGPASS")
    equal(unauth, ["AUTH", "admin", "root-secret"], b"OK")
    equal(unauth, ["ACL", "WHOAMI"], b"admin")
    equal(unauth, ["CLIENT", "SETNAME", "original"], b"OK")
    failure(unauth, ["HELLO", 3, "AUTH", "scoped", "scoped-secret", "SETNAME", "bad name"])
    assert unauth.last_kind == b"-"
    equal(unauth, ["ACL", "WHOAMI"], b"admin")
    candidate = server.connect(context)
    failure(candidate, ["HELLO", 3, "AUTH", "admin", "root-secret", "SETNAME", "bad name"])
    failure(candidate, ["GET", "x"], b"NOAUTH")
    equal(unauth, ["CLIENT", "GETNAME"], b"original")
    assert hello_map(unauth.command("HELLO"))[b"proto"] == 2 and unauth.last_kind == b"*"
    failure(unauth, ["AUTH", "scoped", "wrong"], b"WRONGPASS")
    equal(unauth, ["ACL", "WHOAMI"], b"admin")
    reply = hello_map(unauth.command("HELLO", 3, "AUTH", "admin", "root-secret", "SETNAME", "secure"))
    assert reply[b"proto"] == 3 and reply[b"role"] == b"master" and unauth.last_kind == b"%", reply
    failure(unauth, ["HELLO", 2, "AUTH", "admin", "wrong", "SETNAME", "not-committed"], b"WRONGPASS")
    assert hello_map(unauth.command("HELLO"))[b"proto"] == 3 and unauth.last_kind == b"%"
    equal(unauth, ["CLIENT", "GETNAME"], b"secure")

    prefix = b"scope:\x00\xff:"
    key, other, outside = prefix + b"a", prefix + b"b", b"outside:secret"
    equal(unauth, ["MSET", key, "first", other, "second", outside, "hidden"], b"OK")
    scoped = server.connect(context)
    response = hello_map(scoped.command("HELLO", 2, "AUTH", "scoped", "scoped-secret", "SETNAME", "reader"))
    assert response[b"proto"] == 2, response
    equal(scoped, ["CLIENT", "GETNAME"], b"reader")
    failure(scoped, ["CLIENT", "LIST"], b"NOPERM")
    equal(scoped, ["MGET", key, other], [b"first", b"second"])
    failure(scoped, ["MGET", key, outside], b"NOPERM")
    failure(scoped, ["WATCH", key, outside], b"NOPERM")
    equal(scoped, ["WATCH", key], b"OK")
    equal(scoped, ["UNWATCH"], b"OK")
    for args in (["SCAN", 0], ["KEYS", "*"], ["DBSIZE"], ["FFKV.STATS"], ["FFKV.BACKUP", "forbidden"]):
        failure(scoped, args, b"NOPERM")
    failure(scoped, ["EVAL", "return redis.call('GET',KEYS[1])", 1, outside], b"NOPERM")
    reply = scoped.command("EVAL", "return redis.call('GET',ARGV[1])", 1, key, outside)
    assert has_permission_error(reply), reply
    equal(scoped, ["EVAL", "return redis.call('GET',KEYS[1])", 1, key], b"first")
    equal(scoped, ["MULTI"], b"OK")
    equal(scoped, ["EVAL", "return redis.call('SET',KEYS[1],'forbidden')", 1, key], b"QUEUED")
    result = scoped.command("EXEC")
    assert has_permission_error(result), result
    equal(unauth, ["GET", key], b"first")
    for reconfiguration in (["AUTH", "admin", "root-secret"], ["HELLO", 3], ["CLIENT", "SETNAME", "bad"], ["ACL", "WHOAMI"]):
        equal(scoped, ["MULTI"], b"OK")
        failure(scoped, reconfiguration)
        failure(scoped, ["EXEC"], b"EXECABORT")
        equal(scoped, ["CLIENT", "GETNAME"], b"reader")
    equal(scoped, ["MULTI"], b"OK")
    failure(scoped, ["GET", outside], b"NOPERM")
    failure(scoped, ["EXEC"], b"EXECABORT")
    equal(scoped, ["MULTI"], b"OK")
    failure(scoped, ["UNRECOGNIZED_SECURITY_TEST"])
    failure(scoped, ["EXEC"], b"EXECABORT")
    equal(scoped, ["MULTI"], b"OK")
    failure(scoped, ["GET"])
    equal(scoped, ["DISCARD"], b"OK")

    writer = server.connect(context)
    equal(writer, ["AUTH", "writer", "writer-secret"], b"OK")
    equal(writer, ["MSET", key, "changed", other, "changed-too"], b"OK")
    failure(writer, ["MSET", key, "must-not-change", outside, "must-not-change"], b"NOPERM")
    failure(writer, ["RENAME", key, outside], b"NOPERM")
    failure(writer, ["DEL", key, outside], b"NOPERM")
    failure(writer, ["BLPOP", key, outside, 0.01], b"NOPERM")
    failure(writer, ["FLUSHDB"], b"NOPERM")
    equal(unauth, ["MGET", key, outside], [b"changed", b"hidden"])
    limited = server.connect(context)
    for _ in range(16):
        failure(limited, ["AUTH", "admin", "wrong"], b"WRONGPASS")
    limited.socket.sendall(Client.frame(["PING"]))
    assert limited.file.read(1) == b"", "failed-auth budget did not close the connection"
    print("PASS NOAUTH, generic bounded AUTH, atomic HELLO, binary multi-key ACL, Lua and transaction identity", flush=True)


def generate_certificate(directory, stem):
    cert, key = directory / (stem + ".crt"), directory / (stem + ".key")
    config = directory / (stem + ".conf")
    config.write_text("[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=DNS:localhost,IP:127.0.0.1\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-config", str(config), "-keyout", str(key), "-out", str(cert)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    key.chmod(0o600)
    cert.chmod(0o644)
    return cert, key


def startup_failure(binary, flags):
    process = subprocess.Popen([binary, "--bind", "127.0.0.1:0", *map(str, flags)], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        try:
            _, diagnostics = process.communicate(timeout=7)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
            raise AssertionError("invalid security configuration did not fail startup") from None
        assert process.returncode != 0 and b"listening on " not in diagnostics, diagnostics
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stderr.close()


def tls_scenario(binary, directory, acl):
    cert, key = generate_certificate(directory, "server")
    other_cert, other_key = generate_certificate(directory, "other")
    context = ssl.create_default_context(cafile=str(cert))
    with SecurityServer(binary, "--acl-file", acl, "--tls-cert", cert, "--tls-key", key) as server:
        acl_scenario(server, context)
        for protocol in (ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3):
            version_context = ssl.create_default_context(cafile=str(cert))
            version_context.minimum_version = version_context.maximum_version = protocol
            client = server.connect(version_context)
            equal(client, ["AUTH", "admin", "root-secret"], b"OK")
            equal(client, ["PING"], b"PONG")
        for bad_context, hostname in ((ssl.create_default_context(), "localhost"), (context, "wrong.example"), (ssl.create_default_context(cafile=str(other_cert)), "localhost")):
            try:
                server.connect(bad_context, hostname)
            except ssl.SSLCertVerificationError:
                pass
            else:
                raise AssertionError("untrusted or wrong-host certificate was accepted")
        with socket.create_connection(server.address, timeout=7) as plain:
            plain.settimeout(7)
            plain.sendall(Client.frame(["PING"]))
            try:
                response = plain.recv(128)
            except ConnectionResetError:
                response = b""
            assert b"PONG" not in response and not response.startswith((b"+", b"-")), response
        with socket.create_connection(server.address, timeout=7) as idle:
            idle.settimeout(7)
            assert idle.recv(1) == b"", "idle TLS handshake did not time out"
    for bad_cert, bad_key in ((cert, other_key), (other_key, key), (cert, cert)):
        startup_failure(binary, ["--tls-cert", bad_cert, "--tls-key", bad_key])
    symlink = directory / "key-link"
    symlink.symlink_to(key)
    startup_failure(binary, ["--tls-cert", cert, "--tls-key", symlink])
    key.chmod(0o622)
    startup_failure(binary, ["--tls-cert", cert, "--tls-key", key])
    key.chmod(0o600)
    oversized = directory / "oversized.pem"
    oversized.write_bytes(b"x" * (1024 * 1024 + 1))
    startup_failure(binary, ["--tls-cert", oversized, "--tls-key", key])
    if hasattr(os, "mkfifo"):
        fifo = directory / "key-fifo"
        os.mkfifo(fifo, 0o600)
        startup_failure(binary, ["--tls-cert", cert, "--tls-key", fifo])
    startup_failure(binary, ["--tls-cert", cert])
    startup_failure(binary, ["--tls-key", key])
    print("PASS TLS1.2/1.3, verified chain/hostname, no plaintext fallback, handshake timeout and secure material loading", flush=True)


def default_users(binary, directory):
    acl = directory / "default.acl"
    acl.write_bytes(b"user default on nopass +@all ~*\n")
    with SecurityServer(binary, "--acl-file", acl) as server:
        client = server.connect()
        equal(client, ["PING"], b"PONG")
        equal(client, ["ACL", "WHOAMI"], b"default")
    acl.write_bytes(b"user default on >default-secret +@all ~*\n")
    with SecurityServer(binary, "--acl-file", acl) as server:
        client = server.connect()
        failure(client, ["PING"], b"NOAUTH")
        equal(client, ["AUTH", "default-secret"], b"OK")
        idle = server.connect()
        idle.socket.settimeout(12)
        assert idle.file.read(1) == b"", "initial authentication deadline did not close idle client"
    acl.write_bytes(b"user admin on >root-secret +@all ~*\n")
    with SecurityServer(binary, "--acl-file", acl) as server:
        failure(server.connect(), ["PING"], b"NOAUTH")
    with SecurityServer(binary) as server:
        client = server.connect()
        equal(client, ["PING"], b"PONG")
        failure(client, ["AUTH", "not-configured"])
    print("PASS explicit default nopass, password-only AUTH, absent/default-off fail-closed and no AUTH fallback", flush=True)


def readonly_role(binary, directory, acl):
    # A cold real follower must advertise slave before the initial snapshot and
    # reject writes even while the source is unavailable. No role is simulated.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        unused_port = reservation.getsockname()[1]
    token = directory / "replica.token"
    token.write_text("security-replication-token\n")
    token.chmod(0o600)
    with SecurityServer(binary, "--acl-file", acl,
                        "--data-dir", directory / "replica-data",
                        "--replica-source", f"http://127.0.0.1:{unused_port}",
                        "--replica-token-file", token, "--replica-allow-loopback-http",
                        "--replica-interval-ms", "100", "--replica-timeout-ms", "2000") as server:
        client = server.connect()
        hello = hello_map(client.command("HELLO", 3, "AUTH", "admin", "root-secret"))
        assert hello[b"role"] == b"slave" and hello[b"proto"] == 3, hello
        failure(client, ["SET", "forbidden", "value"], b"READONLY")
        failure(client, ["GET", "cold-data"], b"LOADING")
    print("PASS actual follower HELLO role and READONLY before initial synchronization", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    options = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="ffkv-security-") as temporary:
        directory = Path(temporary)
        acl = directory / "security.acl"
        acl.write_bytes(
            b"user default off nopass +@all ~*\n"
            b"user admin on >root-secret +@all ~*\n"
            b"user scoped on >scoped-secret +@connection +@read +@scripting +@transaction -client|list ~scope:\x00\xff:*\n"
            b"user writer on >writer-secret +@connection +@read +@write ~scope:\x00\xff:*\n"
        )
        with SecurityServer(options.binary, "--acl-file", acl) as server:
            acl_scenario(server, None)
        tls_scenario(options.binary, directory, acl)
        default_users(options.binary, directory)
        readonly_role(options.binary, directory, acl)
    print("PASS security smoke complete", flush=True)


if __name__ == "__main__":
    main()
