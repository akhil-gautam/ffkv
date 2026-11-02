#!/usr/bin/env python3
"""Real authenticated polling replicas; not PSYNC/HA or automatic failover.

Usage: python3 tests/replication_smoke.py [--binary target/debug/ffkv]
Runs loopback HTTP and verified TLS when openssl is installed. All processes,
private tokens, certificates, datasets and deliberate outages are owned fixtures.
Promotion is tested only after stopping/fencing the source. No Redis dependency.
"""
import argparse
import base64
import hashlib
import http.client
import io
import json
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import tempfile
import tarfile
import time

from durability_smoke import Server, passed
from native_queues_smoke import connect, enqueue, claim, get, mapping


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def until(description, operation, timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = operation()
        if last:
            return last
        time.sleep(0.05)
    raise AssertionError(("timed out", description, last))


def error(value, prefix):
    assert isinstance(value, tuple) and value[0] == "error" and value[1].startswith(prefix), value


class API:
    def __init__(self, number, token, context=None):
        self.port, self.token, self.context = number, token, context

    def request(self, path, body=None, token=None):
        headers = {"Authorization": "Bearer " + (self.token if token is None else token)}
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(body)
        connection = (http.client.HTTPSConnection("127.0.0.1", self.port, context=self.context, timeout=5)
                      if self.context else http.client.HTTPConnection("127.0.0.1", self.port, timeout=5))
        try:
            connection.request("POST" if body is not None else "GET", path, body=body, headers=headers)
            response = connection.getresponse()
            data = response.read(16 * 1024 * 1024 + 1)
            assert len(data) <= 16 * 1024 * 1024, "smoke response exceeds fixture bound"
            return response.status, dict(response.getheaders()), data
        finally:
            connection.close()

    def json(self, path, body=None, token=None):
        status, headers, data = self.request(path, body, token)
        return status, json.loads(data)

    def ready(self):
        try:
            status, _ = self.json("/api/session")
            return status == 200
        except (OSError, http.client.HTTPException):
            return False

    def metrics(self):
        status, value = self.json("/api/overview")
        assert status == 200, value
        return value["metrics"]["replication"]

    def console(self, *args):
        return self.json("/api/command", {"args_b64": [base64.b64encode(str(arg).encode()).decode() for arg in args], "confirmed": True, "reveal": True})


def policy(root):
    admin, viewer = "replica-smoke-admin-token", "replica-smoke-viewer-token"
    path = root / "management-tokens.json"
    path.write_text(json.dumps({"tokens": [{"name": name, "role": role, "sha256": hashlib.sha256(value.encode()).hexdigest()}
                                           for name, role, value in [("admin", "admin", admin), ("viewer", "viewer", viewer)]]}))
    path.chmod(0o600)
    raw = root / "replication.token"
    raw.write_text(admin + "\n")
    raw.chmod(0o600)
    return path, raw, admin, viewer


def tls_files(root):
    openssl = shutil.which("openssl")
    if not openssl:
        return None
    config = root / "openssl.cnf"
    config.write_text("[req]\ndistinguished_name=dn\nx509_extensions=ext\nprompt=no\n[dn]\nCN=localhost\n[ext]\nsubjectAltName=IP:127.0.0.1\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    certificate, key = root / "certificate.pem", root / "key.pem"
    subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-config", str(config),
                    "-keyout", str(key), "-out", str(certificate)], check=True, capture_output=True)
    key.chmod(0o600)
    return certificate, key


def scenario(binary, root, tls=None):
    root.mkdir()
    tokens, token_file, admin, viewer = policy(root)
    source_port, follower_port = port(), port()
    context = ssl.create_default_context(cafile=str(tls[0])) if tls else None
    source_api, follower_api = API(source_port, admin, context), API(follower_port, admin, context)

    def management(number, name, export=False):
        args = ["--management-bind", f"127.0.0.1:{number}", "--management-token-file", str(tokens),
                "--management-audit-file", str(root / (name + "-audit.jsonl"))]
        if export:
            args += ["--replication-export"]
        if tls:
            args += ["--management-tls-cert", str(tls[0]), "--management-tls-key", str(tls[1])]
        return args

    source_args = management(source_port, "source", True)
    follower_management = management(follower_port, "follower")
    replica_args = ["--replica-source", f"{'https' if tls else 'http'}://127.0.0.1:{source_port}",
                    "--replica-token-file", str(token_file), "--replica-interval-ms", "100", "--replica-timeout-ms", "2000",
                    "--replica-max-snapshot-bytes", str(16 * 1024 * 1024)]
    replica_args += ["--replica-ca-file", str(tls[0])] if tls else ["--replica-allow-loopback-http"]
    source = Server(binary, root / "source", "strict", extra_args=source_args)
    follower = Server(binary, root / "follower", "strict", extra_args=follower_management + replica_args)
    try:
        follower.start()  # No source exists yet: never expose the cold local dataset.
        fc = connect(follower)
        error(fc.command("GET", "value"), b"LOADING")
        error(fc.command("SET", "value", "forbidden"), b"READONLY")
        until("follower management startup", follower_api.ready)
        # Vendor health remains queryable before the first full snapshot.
        initial = mapping(fc.command("FFKV.STATS"))
        assert mapping(initial[b"server"])[b"role"] == b"slave", initial
        source.start()
        sc = connect(source)
        until("source management startup", source_api.ready)
        if tls:
            untrusted = API(source_port, admin, ssl.create_default_context())
            try:
                untrusted.request("/api/replication/status")
            except ssl.SSLCertVerificationError:
                pass  # Expected refusal, never a permissive TLS fallback.
            else:
                raise AssertionError("untrusted self-signed source certificate was accepted")
        assert sc.command("SET", "value", "first") == b"OK"
        assert sc.command("RPUSH", "list", "one", "two") == 2
        assert sc.command("HSET", "hash", "field", "payload") == 1
        assert sc.command("SADD", "set", "member") == 1
        assert sc.command("ZADD", "zset", 7, "member") == 1
        absolute_ttl = int(time.time() * 1000) + 60000
        assert sc.command("SET", "ttl", "temporary", "PXAT", absolute_ttl) == b"OK"
        assert not isinstance(sc.command("FQ.CONFIG", "queue", "IDEMPOTENCY", 600000), tuple)
        job = enqueue(sc, "queue", b"native\x00payload", "IDEMPOTENCY", "dedupe")[b"job_id"]
        row, = claim(sc, "queue", "VISIBILITY", 600000)
        expected_job = get(sc, "queue", job)
        assert expected_job[b"state"] == b"running"
        live_job = enqueue(sc, "live-queue", b"keep receipt", "IDEMPOTENCY", "live-dedupe")[b"job_id"]
        live_row, = claim(sc, "live-queue", "VISIBILITY", 600000)
        assert sc.command("SET", "barrier", "seeded") == b"OK"
        until("initial dataset", lambda: fc.command("GET", "barrier") == b"seeded")
        assert fc.command("GET", "value") == b"first"
        assert fc.command("LRANGE", "list", 0, -1) == [b"one", b"two"]
        assert fc.command("HGET", "hash", "field") == b"payload"
        assert fc.command("SMEMBERS", "set") == [b"member"]
        assert fc.command("ZSCORE", "zset", "member") in (b"7", 7.0)
        assert abs(fc.command("PTTL", "ttl") - (absolute_ttl - int(time.time() * 1000))) < 250
        assert get(fc, "queue", job) == expected_job
        assert mapping(mapping(fc.command("FFKV.STATS"))[b"server"])[b"role"] == b"slave"
        error(fc.command("SET", "value", "wrong"), b"READONLY")
        error(fc.command("EVAL", "return redis.call('SET', KEYS[1], 'wrong')", 1, "value"), b"READONLY")
        error(fc.command("FQ.ACK", "queue", row[b"receipt"]), b"READONLY")
        assert fc.command("MULTI") == b"OK"
        queued = fc.command("SET", "value", "wrong")
        assert queued == b"QUEUED" or (isinstance(queued, tuple) and queued[1].startswith(b"READONLY")), queued
        executed = fc.command("EXEC")
        assert (isinstance(executed, tuple) and executed[1].startswith((b"READONLY", b"EXECABORT"))) or (isinstance(executed, list) and all(isinstance(v, tuple) and v[1].startswith(b"READONLY") for v in executed)), executed
        status, denied = follower_api.console("SET", "value", "wrong")
        assert status in (200, 400, 403), (status, denied)
        if status == 200:
            assert denied["result"]["type"] == "error", denied
        assert fc.command("GET", "value") == b"first"
        status, _ = source_api.json("/api/replication/status", token="invalid")
        assert status == 401
        status, _ = source_api.json("/api/replication/status", token=viewer)
        assert status == 403
        status, _, _ = source_api.request("/api/replication/snapshot", token=viewer)
        assert status == 403
        status, _ = follower_api.json("/api/replication/snapshot")
        assert status in (403, 503)  # A slave must never become a source.
        status, before = source_api.json("/api/replication/status")
        assert status == 200 and before["role"] == "master" and before["format"] == 1
        status, headers, archive = source_api.request("/api/replication/snapshot")
        assert status == 200 and headers["x-ffkv-instance-id"] == before["instance_id"]
        assert int(headers["x-ffkv-revision"]) == before["revision"]
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as exported:
            members = exported.getmembers()
            assert [member.name for member in members] == ["manifest.ffkv", "wal.ffkv"]
            assert all(member.isreg() for member in members)
            manifest = exported.extractfile("manifest.ffkv").read()
            wal = exported.extractfile("wal.ffkv").read()
            assert len(manifest) == 88 and manifest[:8] == b"FFKVBAK1"
            assert int.from_bytes(manifest[8:16], "little") == len(wal)
            assert hashlib.sha256(wal).digest() == manifest[24:56]
            assert hashlib.sha256(manifest[:56]).digest() == manifest[56:]
        assert not isinstance(sc.command("FFKV.CHECKPOINT"), tuple)
        status, after = source_api.json("/api/replication/status")
        assert status == 200 and after["revision"] == before["revision"] and after["instance_id"] == before["instance_id"], (before, after)
        # Checkpoint file-local WAL sequences must not hide later logical writes.
        assert sc.command("SET", "value", "second") == b"OK"
        assert sc.command("FQ.ACK", "queue", row[b"receipt"]) == b"OK"
        assert sc.command("SET", "barrier", "changed") == b"OK"
        until("incremental source change via full poll", lambda: fc.command("GET", "barrier") == b"changed")
        assert fc.command("GET", "value") == b"second"
        assert get(fc, "queue", job) == get(sc, "queue", job)
        until("connected zero observed lag", lambda: follower_api.metrics()["status"] == "connected" and follower_api.metrics()["lag"] == 0)
        # This operator backup is not export-owned and must survive polling.
        operator = mapping(sc.command("FFKV.BACKUP", "operator-kept"))
        assert Path(operator[b"path"].decode()).is_dir()
        # Drop an actual authenticated response before consuming its TAR body.
        disconnected = (http.client.HTTPSConnection("127.0.0.1", source_port, context=context, timeout=5)
                        if tls else http.client.HTTPConnection("127.0.0.1", source_port, timeout=5))
        try:
            disconnected.request("GET", "/api/replication/snapshot", headers={"Authorization": "Bearer " + admin})
            response = disconnected.getresponse()
            assert response.status == 200
            response.close()
        finally:
            disconnected.close()
        until("export cleanup", lambda: not list((root / "source" / "backups").glob("replication-export-*")))
        audit_path = root / "source-audit.jsonl"
        def export_intents():
            return sum(json.loads(line)["operation"] == "replication-export" and json.loads(line)["outcome"] == "intent"
                       for line in audit_path.read_text().splitlines())
        stable_count = export_intents()
        time.sleep(0.5)  # Several real network status polls with no source mutations.
        assert export_intents() == stable_count, "unchanged revision triggered an unnecessary snapshot"
        source.stop()
        until("visible source outage", lambda: follower_api.metrics()["status"] == "disconnected")
        assert fc.command("GET", "value") == b"second"  # Explicit stale-read policy.
        # Fresh process must not silently bless a recovered cold copy as current.
        follower.stop()
        follower.start()
        fc = connect(follower)
        error(fc.command("GET", "value"), b"LOADING")
        source.start()
        sc = connect(source)
        assert sc.command("SET", "barrier", "rebooted") == b"OK"
        until("source reboot forces fresh install", lambda: fc.command("GET", "barrier") == b"rebooted", timeout=35)
        assert fc.command("GET", "value") == b"second"
        status, rebooted = source_api.json("/api/replication/status")
        assert status == 200 and rebooted["instance_id"] != before["instance_id"]
        assert (root / "source" / "backups" / "operator-kept").is_dir()
        # FENCE FIRST: never test an unfenced writable pair or claim election.
        source.stop()
        follower.stop()
        follower.extra_args = tuple(follower_management)  # Manual restart without replica source.
        follower.start()
        promoted = connect(follower)
        assert promoted.command("GET", "value") == b"second"
        assert get(promoted, "live-queue", live_job)[b"state"] == b"running"
        assert promoted.command("FQ.ACK", "live-queue", live_row[b"receipt"]) == b"OK"
        assert enqueue(promoted, "queue", b"do not duplicate", "IDEMPOTENCY", "dedupe") == {b"job_id": job, b"created": 0}
        assert promoted.command("SET", "value", "promoted") == b"OK"
        assert promoted.command("GET", "value") == b"promoted"
        audit_rows = [json.loads(line) for line in (root / "source-audit.jsonl").read_text().splitlines()]
        exports = [row for row in audit_rows if row["operation"] == "replication-export"]
        assert any(row["outcome"] == "intent" for row in exports) and any(row["outcome"] == "streamed" for row in exports), exports
        assert admin not in json.dumps(audit_rows) and "native\\u0000payload" not in json.dumps(audit_rows)
        passed("verified TLS network replica" if tls else "HTTP bearer network replica")
    finally:
        source.cleanup()
        follower.cleanup()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/ffkv")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    with tempfile.TemporaryDirectory(prefix="ffkv-replication-") as temporary:
        root = Path(temporary)
        scenario(binary, root / "http")
        tls = tls_files(root)
        if tls:
            scenario(binary, root / "tls", tls)
        else:
            print("SKIP verified TLS scenario: openssl is unavailable", flush=True)


if __name__ == "__main__":
    main()
