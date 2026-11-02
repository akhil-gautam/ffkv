#!/usr/bin/env python3
"""Run unmodified pinned Sidekiq CLI and session-backed Web against owned ffkv.

Prepare once: BUNDLE_GEMFILE=tests/sidekiq/Gemfile bundle lock --local
Run: python3 tests/sidekiq_smoke.py --binary target/debug/ffkv
No Redis installation, existing endpoint, fake fetcher, or inline testing mode.
Failures retain subprocess stdout/stderr and HTTP bodies under --artifact-dir.
"""
import argparse
import contextlib
import http.cookiejar
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request
import uuid

from durability_smoke import Server


FIXTURES = Path(__file__).resolve().parent / "sidekiq"


def wait_for(description, predicate, processes=(), timeout=35):
    deadline = time.monotonic() + timeout
    while True:
        for process in processes:
            assert process.process.poll() is None, (
                description, process.name, "exited", process.process.returncode
            )
        value = predicate()
        if value:
            return value
        assert time.monotonic() < deadline, f"timed out: {description}"
        time.sleep(0.05)


class LoggedProcess:
    def __init__(self, name, args, env, logs):
        self.name = name
        self.stdout = (logs / f"{name}.stdout.log").open("wb")
        self.stderr = (logs / f"{name}.stderr.log").open("wb")
        try:
            self.process = subprocess.Popen(
                args, cwd=FIXTURES, env=env, stdout=self.stdout, stderr=self.stderr,
            )
        except BaseException:
            self.stdout.close()
            self.stderr.close()
            raise

    def stop(self):
        try:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
                    raise AssertionError(f"{self.name} did not stop after SIGTERM") from None
            assert self.process.returncode == 0, (
                self.name, "exit status", self.process.returncode
            )
        finally:
            self.stdout.close()
            self.stderr.close()

    def cleanup(self):
        if self.process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        self.stdout.close()
        self.stderr.close()


class DiagnosticServer(Server):
    def __init__(self, *args, log_path, **kwargs):
        super().__init__(*args, **kwargs)
        self.log_path = log_path

    def release_process(self):
        if self.process is not None and self.process.poll() is not None:
            with self.log_path.open("ab") as output:
                output.write(self.process.stderr.read())
        super().release_process()


def ruby_action(action, env, logs):
    # File-backed output cannot fill a pipe if Ruby emits a large exception.
    stdout = logs / f"client-{action}.stdout.log"
    stderr = logs / f"client-{action}.stderr.log"
    with stdout.open("wb") as out, stderr.open("wb") as err:
        process = subprocess.Popen(
            ["bundle", "exec", "ruby", str(FIXTURES / "client.rb"), action],
            env=env, cwd=FIXTURES, stdout=out, stderr=err,
        )
        try:
            code = process.wait(timeout=20)
        except BaseException:
            process.kill()
            process.wait(timeout=5)
            raise
    assert code == 0, ("Ruby client action", action, code)
    return json.loads(stdout.read_text())


def raw_hash(client, key):
    reply = client.command("HGETALL", key)
    assert isinstance(reply, list) and len(reply) % 2 == 0, (key, reply)
    return dict(zip(reply[::2], reply[1::2]))


def exercise(server, root, logs):
    prefix = "ffkv:test:sidekiq:" + uuid.uuid4().hex
    queue = "fixture_" + uuid.uuid4().hex
    held_queue = queue + "_held"
    tag = "sidekiq-fixture-" + uuid.uuid4().hex
    env = os.environ.copy()
    env.update({
        "BUNDLE_GEMFILE": str(FIXTURES / "Gemfile"),
        "FFKV_SIDEKIQ_URL": f"redis://{server.address[0]}:{server.address[1]}/0",
        "FFKV_SIDEKIQ_PREFIX": prefix,
        "FFKV_SIDEKIQ_QUEUE": queue,
        "FFKV_SIDEKIQ_WEB_ADDRESS": str(root / "web-address.json"),
        "RACK_ENV": "production",
    })
    client = server.connect()
    ids = ruby_action("seed", env, logs)
    assert set(ids) == {"immediate", "scheduled", "retry", "held", "future"}, ids
    assert all(isinstance(jid, str) and len(jid) == 24 for jid in ids.values()), ids
    assert len(set(ids.values())) == 5, ids
    assert client.command("SCARD", "queues") == 2
    assert set(client.command("SMEMBERS", "queues")) == {queue.encode(), held_queue.encode()}
    assert client.command("LLEN", "queue:" + queue) == 2
    assert client.command("ZCARD", "schedule") == 2
    assert client.command("ZCARD", "retry") == 0
    assert client.command("GET", prefix + ":attempts") is None

    with contextlib.ExitStack() as resources:
        web = LoggedProcess("web", ["bundle", "exec", "ruby", str(FIXTURES / "web.rb")], env, logs)
        resources.callback(web.cleanup)
        ready = root / "web-address.json"

        def web_address():
            if not ready.exists():
                return None
            try:
                return json.loads(ready.read_text())
            except json.JSONDecodeError:
                return None

        address = wait_for("Puma loopback listener", web_address, [web], timeout=20)
        assert address["host"] == "127.0.0.1" and 0 < address["port"] < 65536, address
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),
        )
        request_count = 0

        def page(path, *markers, as_json=False):
            nonlocal request_count
            request_count += 1
            url = f"http://127.0.0.1:{address['port']}{path}"
            with opener.open(url, timeout=5) as response:
                body = response.read().decode("utf-8")
                (logs / f"http-{request_count:02}.body").write_text(body)
                assert response.status == 200, (path, response.status)
                content_type = response.headers.get("Content-Type", "")
                assert ("json" if as_json else "html") in content_type, (path, content_type)
            for marker in markers:
                assert str(marker) in body, (path, "missing fixture", marker)
            return json.loads(body) if as_json else body

        # Before starting workers, prove pending queue and scheduled payloads are
        # rendered by the real Web app, not just a working HTTP listener.
        page("/", "Sidekiq", "Processed", "Failed")
        page("/queues", held_queue, queue)
        page("/queues/" + held_queue, "FixtureJob", "held_queue_marker", ids["held"])
        page("/scheduled", "FixtureJob", "future_schedule_marker", ids["future"])

        worker = LoggedProcess("worker", [
            "bundle", "exec", "sidekiq", "-r", str(FIXTURES / "jobs.rb"),
            "-c", "2", "-q", queue, "-t", "5", "-g", tag,
        ], env, logs)
        resources.callback(worker.cleanup)
        processes = [worker, web]

        def registered_process():
            identities = client.command("SMEMBERS", "processes")
            assert isinstance(identities, list), identities
            return identities[0] if len(identities) == 1 else None

        identity = wait_for("worker heartbeat registration", registered_process, processes)
        metadata = raw_hash(client, identity)
        info = json.loads(metadata[b"info"])
        assert info["pid"] == worker.process.pid, info
        assert info["tag"] == tag and info["concurrency"] == 2, info
        assert info["queues"] == [queue] and info["version"] == "8.0.7", info
        first_beat = float(metadata[b"beat"])
        assert 0 < client.command("TTL", identity) <= 60
        page("/busy", tag, worker.process.pid, queue)

        def retry_entry():
            entries = client.command("ZRANGE", "retry", 0, -1)
            assert isinstance(entries, list), entries
            return next((json.loads(value) for value in entries
                         if json.loads(value)["jid"] == ids["retry"]), None)

        retry = wait_for("first failed attempt in real retry set", retry_entry, processes)
        assert retry["error_class"] == "FixtureRetryError" and retry["retry_count"] == 0, retry
        assert retry["class"] == "FixtureRetryJob" and retry["queue"] == queue, retry
        page("/retries", "FixtureRetryJob", "FixtureRetryError", ids["retry"])
        print("PASS actual Sidekiq retry payload and HTTP retries page", flush=True)

        def completed():
            values = raw_hash(client, prefix + ":results")
            return values if set(values) == {b"immediate", b"scheduled", b"retry"} else None

        results = wait_for("immediate, scheduled and retried job results", completed, processes)
        decoded = {key.decode(): json.loads(value) for key, value in results.items()}
        for kind, value in decoded.items():
            assert value == {
                "kind": kind, "attempt": 2 if kind == "retry" else 1,
                "jid": ids[kind], "token": prefix, "pid": worker.process.pid,
            }, value
        assert client.command("GET", prefix + ":attempts") == b"2"
        assert client.command("LLEN", "queue:" + queue) == 0
        assert client.command("LLEN", "queue:" + held_queue) == 1
        assert client.command("ZCARD", "retry") == 0
        assert client.command("ZCARD", "schedule") == 1
        assert client.command("ZCARD", "dead") == 0
        future = json.loads(client.command("ZRANGE", "schedule", 0, -1)[0])
        assert future["jid"] == ids["future"] and future["args"] == ["future_schedule_marker"], future

        def flushed_and_beating():
            beat = client.command("HGET", identity, "beat")
            return (beat is not None and float(beat) > first_beat
                    and client.command("GET", "stat:processed") == b"4"
                    and client.command("GET", "stat:failed") == b"1")

        # The genuine heartbeat runs every ten seconds. Count all four attempts,
        # including the intentional failed attempt, without forcing a flush.
        wait_for("periodic heartbeat and exact attempt statistics", flushed_and_beating, processes)
        stats = page("/stats", as_json=True)["sidekiq"]
        assert stats == {
            "processed": 4, "failed": 1, "busy": 0, "processes": 1,
            "enqueued": 1, "scheduled": 1, "retries": 0, "dead": 0,
            "default_latency": 0.0,
        }, stats
        assert page("/stats/queues", as_json=True) == {queue: 0, held_queue: 1}
        page("/", "Sidekiq", "Processed", "Failed")
        page("/queues/" + held_queue, "held_queue_marker", ids["held"])
        page("/scheduled", "future_schedule_marker", ids["future"])
        page("/busy", tag, worker.process.pid)

        snapshot = ruby_action("snapshot", env, logs)
        assert snapshot["versions"] == {
            "ruby": "3.4.6", "sidekiq": "8.0.7", "redis-client": "0.26.2"
        }, snapshot["versions"]
        assert snapshot["results"] == decoded and snapshot["attempts"] == 2, snapshot
        assert snapshot["queues"] == {queue: 0, held_queue: 1}, snapshot
        assert snapshot["retries"] == [] and len(snapshot["scheduled"]) == 1, snapshot
        assert snapshot["scheduled"][0]["jid"] == ids["future"], snapshot
        assert snapshot["stats"] == {
            "processed": 4, "failed": 1, "enqueued": 1,
            "scheduled": 1, "retries": 0, "dead": 0,
        }, snapshot
        assert len(snapshot["processes"]) == 1, snapshot
        assert snapshot["processes"][0]["pid"] == worker.process.pid, snapshot
        assert snapshot["errors"] == [{"class": "FixtureRetryError", "jid": ids["retry"]}], snapshot
        print("PASS real CLI jobs, redis-client results, heartbeat, queue/set/stats bookkeeping and Web", flush=True)

        worker.stop()
        assert client.command("SCARD", "processes") == 0
        assert client.command("EXISTS", identity + b":work") == 0
        assert client.command("GET", "stat:processed") == b"4"
        assert client.command("GET", "stat:failed") == b"1"
        # Exit-time default metric flushing also goes through the error handler.
        errors = [json.loads(value) for value in client.command("LRANGE", prefix + ":errors", 0, -1)]
        assert errors == [{"class": "FixtureRetryError", "jid": ids["retry"]}], errors
        web.stop()
        print("PASS finite Sidekiq/Puma shutdown and process-set cleanup", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    parser.add_argument("--artifact-dir", default="target/sidekiq-smoke-artifacts")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    artifacts = Path(args.artifact_dir).resolve()
    with tempfile.TemporaryDirectory(prefix="ffkv-sidekiq-") as directory:
        root = Path(directory)
        logs = root / "logs"
        logs.mkdir()
        data = root / "data"
        data.mkdir()
        try:
            with DiagnosticServer(
                binary, data, "strict", extra_args=("--shards", "4"),
                log_path=logs / "ffkv.stderr.log",
            ) as server:
                exercise(server, root, logs)
        except BaseException:
            destination = artifacts / uuid.uuid4().hex
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(logs, destination)
            print(f"Failure artifacts: {destination}", flush=True)
            raise
    print("PASS pinned Sidekiq 8.0.7 / redis-client 0.26.2 integration", flush=True)


if __name__ == "__main__":
    main()
