#!/usr/bin/env python3
"""Exercise ffkv-cli against owned ffkv processes; no Redis executable required."""
import argparse
import base64
import contextlib
import json
import fcntl
import os
from pathlib import Path
import pty
import select
import signal
import subprocess
import tempfile
import termios
import time

from durability_smoke import Server
from security_smoke import SecurityServer, generate_certificate


def client_env(**values):
    env = os.environ.copy()
    for name in ("FFKV_URL", "FFKVCLI_AUTH"):
        env.pop(name, None)
    env.update(values)
    return env


def run(binary, address, *args, data=None, code=0, env=None):
    result = subprocess.run(
        [binary, "-h", address[0], "-p", str(address[1]), *map(str, args)],
        input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env or client_env(), timeout=10,
    )
    assert result.returncode == code, (args, result.returncode, result.stdout, result.stderr)
    return result


@contextlib.contextmanager
def owned_process(args, **kwargs):
    child = subprocess.Popen(args, **kwargs)
    try:
        yield child
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=5)


def read_line(child, timeout=5):
    deadline = time.monotonic() + timeout
    value = bytearray()
    while not value.endswith(b"\n"):
        remaining = deadline - time.monotonic()
        assert remaining > 0, ("stream output timeout", bytes(value))
        assert select.select([child.stdout], [], [], remaining)[0], ("stream output timeout", bytes(value))
        byte = child.stdout.read(1)
        assert byte, ("stream output EOF", bytes(value))
        value.extend(byte)
    return bytes(value)


class Terminal:
    def __init__(self, binary, address, args=(), pipe_input=False):
        self.master, slave = pty.openpty()
        self.saved = termios.tcgetattr(slave)
        self.process = subprocess.Popen(
            [binary, "-h", address[0], "-p", str(address[1]), *map(str, args)],
            stdin=subprocess.PIPE if pipe_input else slave, stdout=slave, stderr=slave, env=client_env(TERM="xterm"),
            start_new_session=True, preexec_fn=lambda: fcntl.ioctl(1, termios.TIOCSCTTY, 0),
        )
        os.close(slave)
        self.output = b""

    def send(self, data):
        os.write(self.master, data)

    def expect(self, value, timeout=5):
        deadline = time.monotonic() + timeout
        while value not in self.output:
            assert self.process.poll() is None, ("terminal exited", self.output)
            remaining = deadline - time.monotonic()
            assert remaining > 0, ("terminal timeout", value, self.output)
            if select.select([self.master], [], [], remaining)[0]:
                try:
                    chunk = os.read(self.master, 65536)
                except OSError:
                    chunk = b""
                assert chunk, ("terminal EOF", self.output)
                self.output += chunk
        self.output = self.output.split(value, 1)[1]

    def wait(self, timeout=5):
        deadline = time.monotonic() + timeout
        while self.process.poll() is None:
            remaining = deadline - time.monotonic()
            assert remaining > 0, ("terminal shutdown timeout", self.output)
            if select.select([self.master], [], [], min(remaining, 0.05))[0]:
                with contextlib.suppress(OSError):
                    self.output += os.read(self.master, 65536)
        return self.process.returncode

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)
            raise AssertionError("interactive client did not exit")
        finally:
            if self.process.stdin:
                self.process.stdin.close()
            os.close(self.master)


def direct_and_scripted(binary, server):
    address = server.address
    assert run(binary, address, "PING").stdout == b"PONG\n"
    assert run(binary, address, "--raw", "SET", "cli:value", "hello").stdout == b"OK\n"
    assert run(binary, address, "--raw", "GET", "cli:value").stdout == b"hello\n"
    assert json.loads(run(binary, address, "-3", "--json", "HELLO", "3").stdout)["server"] == "ffkv"
    payload = bytes(range(256)) * 8192 + b"\x00\xff\r\nlast\n"
    assert run(binary, address, "--raw", "-x", "SET", "cli:binary", data=payload).stdout == b"OK\n"
    assert run(binary, address, "--raw", "GET", "cli:binary").stdout == payload + b"\n"
    interleaved = run(binary, address, "--raw", data=b"GET cli:binary\nPING\n").stdout
    assert interleaved == payload + b"\nPONG\n", "queued input lost a fragmented binary reply"
    assert json.loads(run(binary, address, "--json", "GET", "cli:missing").stdout) is None
    session = b'SET cli:quoted "hello world"\nGET cli:quoted\nSET cli:empty ""\nGET cli:empty\nMULTI\nSET cli:tx committed\nGET cli:tx\nEXEC\nGET cli:tx\n'
    replies = run(binary, address, "--json", data=session).stdout.splitlines()
    decoded = [json.loads(reply) for reply in replies]
    assert decoded == ["OK", "hello world", "OK", "", "OK", "QUEUED", "QUEUED", ["OK", "committed"], "committed"], decoded
    malformed = run(binary, address, "--raw", data=b'SET cli:bad "unterminated\nPING\n', code=1)
    assert b"PONG" in malformed.stdout
    assert server.connect().command("EXISTS", "cli:bad") == 0
    nested_error = run(binary, address, "--json", data=b'MULTI\nSET cli:typed string\nLPUSH cli:typed value\nEXEC\n', code=1)
    assert b"WRONGTYPE" in nested_error.stdout
    encoded = json.loads(run(binary, address, "--json", "GET", "cli:binary").stdout)
    assert encoded == {"type": "bytes", "base64": base64.b64encode(payload).decode()}, encoded
    unknown = run(binary, address, "NO_SUCH_COMMAND", code=1).stdout
    assert b"ERR" in unknown and b"unknown command" in unknown, unknown
    print("PASS direct commands, RESP3 maps, exact binary stdin, quoting, transactions and error exit statuses", flush=True)


def protocol_transition(binary, server):
    session = b'HELLO 3\nSET cli:message message\nSUBSCRIBE cli:transition\nMGET cli:message\nUNSUBSCRIBE\nPING\n'
    lines = [json.loads(value) for value in run(binary, server.address, "--json", data=session).stdout.splitlines()]
    assert lines[0]["proto"] == 3 and ["message"] in lines and lines[-1] == "PONG", lines
    print("PASS HELLO protocol transitions do not mistake ordinary arrays for subscription messages", flush=True)

def blocking_and_terminal(binary, server):
    address = server.address
    with owned_process(
        [binary, "-h", address[0], "-p", str(address[1]), "--json", "BRPOP", "cli:wait", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=client_env(),
    ) as consumer:
        assert server.connect().command("LPUSH", "cli:wait", "job") == 1
        out, err = consumer.communicate(timeout=5)
        assert consumer.returncode == 0 and json.loads(out) == ["cli:wait", "job"], (out, err)
    terminal = Terminal(binary, address)
    try:
        terminal.expect(b"> ")
        terminal.send(b"PING\n")
        terminal.expect(b"PONG")
        terminal.send(b'SET cli:interactive "one two"\n')
        terminal.expect(b"OK")
        terminal.send(b"GET cli:interactive\nPING\n")
        terminal.expect(b"one two")
        terminal.expect(b"PONG")
        terminal.send(b"SUBSCRIBE cli:interactive-events\n")
        terminal.expect(b"subscribe")
        terminal.expect(b"cli:interactive-events")
        assert server.connect().command("PUBLISH", "cli:interactive-events", "pushed-while-editing") == 1
        terminal.expect(b"pushed-while-editing")
        terminal.send(b"UNSUBSCRIBE\n")
        terminal.expect(b"unsubscribe")
        terminal.send(b"PING\n")
        terminal.expect(b"PONG")
        terminal.expect(b"ffkv> ")
        terminal.send(b"\x04")
        assert terminal.wait() == 0
    finally:
        terminal.close()
    with owned_process(
        [binary, "-h", address[0], "-p", str(address[1]), "BRPOP", "cli:interrupt", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=client_env(),
    ) as blocked:
        time.sleep(0.1)
        assert blocked.poll() is None
        blocked.send_signal(signal.SIGINT)
        blocked.communicate(timeout=5)
        assert blocked.returncode == 130, blocked.returncode
    print("PASS blocking wakeups, pasted terminal commands, idle Pub/Sub delivery, EOF and interruption", flush=True)


def native_jobs_and_streaming(binary, server):
    address = server.address
    payload = '{"job":"cli-proof","values":[false,null,42]}'
    added = json.loads(run(binary, address, "-3", "--json", "FQ.ENQUEUE", "cli:native", payload).stdout)
    job, = json.loads(run(binary, address, "-3", "--json", "FQ.CLAIM", "cli:native", "VISIBILITY", 60000).stdout)
    assert job["job_id"] == added["job_id"] and job["payload"] == payload and job["attempts"] == 1, job
    assert run(binary, address, "--raw", "FQ.ACK", "cli:native", job["receipt"]).stdout == b"OK\n"
    assert json.loads(run(binary, address, "-3", "--json", "FQ.GET", "cli:native", job["job_id"]).stdout) is None
    for protocol in ("-2", "-3"):
        with owned_process(
            [binary, "-h", address[0], "-p", str(address[1]), protocol, "--raw", "SUBSCRIBE", "cli:events:a", "cli:events:b"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0, env=client_env(),
        ) as subscriber:
            for channel, count in ((b"cli:events:a", b"1"), (b"cli:events:b", b"2")):
                assert [read_line(subscriber) for _ in range(3)] == [b"subscribe\n", channel + b"\n", count + b"\n"]
            for channel in (b"cli:events:a", b"cli:events:b"):
                assert server.connect().command("PUBLISH", channel, b"message-body") == 1
                assert [read_line(subscriber) for _ in range(3)] == [b"message\n", channel + b"\n", b"message-body\n"]
            subscriber.send_signal(signal.SIGINT)
            subscriber.communicate(timeout=5)
            assert subscriber.returncode == 130, subscriber.returncode
    print("PASS native leased-job receipt lifecycle and multi-channel RESP2/RESP3 streaming", flush=True)


def auth_and_tls(binary, server_binary, directory):
    cert, key = generate_certificate(directory, "client-smoke")
    acl = directory / "users.acl"
    secret = "cli-percent+secret/with:punctuation"
    acl.write_text(f"user app on >{secret} allkeys allchannels +@all\n")
    acl.chmod(0o600)
    with SecurityServer(server_binary, "--acl-file", acl, "--tls-cert", cert, "--tls-key", key) as secured:
        flags = ["--tls", "--cacert", str(cert), "--sni", "localhost", "--user", "app"]
        env = client_env(FFKVCLI_AUTH=secret)
        assert run(binary, secured.address, *flags, "PING", env=env).stdout == b"PONG\n"
        assert run(binary, secured.address, "-3", *flags, "PING", env=env).stdout == b"PONG\n"
        wrong = run(binary, secured.address, "--tls", "--cacert", cert, "--sni", "wrong.invalid", "PING", code=2)
        assert secret.encode() not in wrong.stdout + wrong.stderr
        bad_auth = run(binary, secured.address, *flags, "PING", env=client_env(FFKVCLI_AUTH="incorrect"), code=2)
        assert b"incorrect" not in bad_auth.stdout + bad_auth.stderr
        untrusted = run(binary, secured.address, "--tls", "PING", code=2)
        assert b"PONG" not in untrusted.stdout
        from urllib.parse import quote
        uri = f"rediss://app:{quote(secret, safe='')}@127.0.0.1:{secured.address[1]}/0"
        result = subprocess.run(
            [binary, "-u", uri, "--cacert", str(cert), "--sni", "localhost", "PING"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=client_env(), timeout=10,
        )
        assert result.returncode == 0 and result.stdout == b"PONG\n", (result.stdout, result.stderr)
        assert secret.encode() not in result.stderr and uri.encode() not in result.stderr
        reauthenticate = f'SUBSCRIBE cli:auth:a cli:auth:b\nAUTH app {secret}\nUNSUBSCRIBE\nPING\n'.encode()
        result = run(binary, secured.address, "-3", "--raw", *flags, data=reauthenticate, env=env)
        assert result.stdout.endswith(b"PONG\n"), result.stdout
        prompt = Terminal(binary, secured.address, [*flags, "--askpass", "PING"], pipe_input=True)
        try:
            prompt.expect(b"Password:")
            deadline = time.monotonic() + 5
            while termios.tcgetattr(prompt.master)[3] & termios.ECHO:
                assert time.monotonic() < deadline, "password prompt did not disable echo"
                time.sleep(0.005)
            prompt.send(b"\x03")
            assert prompt.wait() == 130
            assert termios.tcgetattr(prompt.master) == prompt.saved, "password interruption did not restore terminal state"
        finally:
            prompt.close()
    print("PASS authenticated RESP2/RESP3, percent-encoded URI secrets, verified TLS/hostname and fail-closed errors", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--client", default="target/debug/ffkv-cli")
    options = parser.parse_args()
    server_binary = str(Path(options.binary).resolve())
    client = str(Path(options.client).resolve())
    with tempfile.TemporaryDirectory(prefix="ffkv-client-") as temp:
        with Server(server_binary) as server:
            direct_and_scripted(client, server)
            protocol_transition(client, server)
            blocking_and_terminal(client, server)
            native_jobs_and_streaming(client, server)
        auth_and_tls(client, server_binary, Path(temp))
    print("PASS ffkv-cli smoke complete; owned processes and fixtures cleaned up", flush=True)


if __name__ == "__main__":
    main()
