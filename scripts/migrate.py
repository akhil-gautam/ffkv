#!/usr/bin/env python3
"""Quiesced, non-atomic Redis -> ffkv copy using only Python's standard library.

The source must have no writers and the target must be empty and exclusively
owned by this migration. SCAN may repeat keys; disk-backed deduplication prevents
list duplication without keeping the keyspace in RAM. Source/target clocks must
be synchronized for absolute key/field expiry. Naturally expiring data is never
made persistent. A failed invocation may leave a partial target: inspect the JSON
result, then start again with a NEW empty target, not this partial database.

TLS is enabled with --source-tls-ca/--target-tls-ca (system trust can instead be
selected with --source-tls/--target-tls). Passwords come only from files; one final
LF is removed. Plaintext is intended only for isolated trusted local endpoints.
Cursor COUNT is a hint, not a hard Redis response bound: an oversized server page
fails explicitly rather than allocating without limit or skipping/truncating it.
"""
import argparse
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import ssl
import sys
import tempfile
import time

MAX_COMMAND = 8 * 1024 * 1024
MAX_ARGS = 1024
MAX_REPLY = 8 * 1024 * 1024
MAX_ARRAY = 16384


class MigrationError(Exception):
    pass


class ServerError(MigrationError):
    pass


def argument(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("ascii")
    return str(value).encode("ascii")


def encode(parts):
    parts = [argument(part) for part in parts]
    if not parts or len(parts) > MAX_ARGS:
        raise MigrationError("command exceeds 1024 arguments")
    size = len(str(len(parts))) + 3
    for part in parts:
        size += len(str(len(part))) + len(part) + 5
    if size > MAX_COMMAND:
        raise MigrationError("oversized value/member/key: encoded command exceeds 8 MiB; nothing is truncated")
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(p) + p + b"\r\n" for p in parts)


def endpoint(value):
    try:
        if value.startswith("["):
            host, remainder = value[1:].split("]", 1)
            if not remainder.startswith(":"):
                raise ValueError()
            port = int(remainder[1:])
        else:
            host, port = value.rsplit(":", 1)
            port = int(port)
            if ":" in host:
                raise ValueError()
        if not host or not 1 <= port <= 65535 or "@" in host or "/" in host:
            raise ValueError()
        return host, port
    except ValueError:
        raise argparse.ArgumentTypeError("expected host:port or [IPv6]:port, never a credential URL") from None


class Client:
    def __init__(self, label, address, username, password_file, tls, ca, server_name, timeout):
        self.label = label
        self.timeout = timeout
        self.socket = None
        self.reader = None
        self.deadline = 0
        self.remaining = 0
        self.reply_nodes = 0
        try:
            password = None
            if password_file:
                with Path(password_file).open("rb") as handle:
                    password = handle.read(16386)
                if password.endswith(b"\n"):
                    password = password[:-1]
                if not password or len(password) > 16384:
                    raise MigrationError(label + " password file must contain 1..16384 bytes")
            if username and password is None:
                raise MigrationError(label + " username requires a password file")
            self.socket = socket.create_connection(address, timeout=timeout)
            if tls or ca:
                context = ssl.create_default_context(cafile=ca)
                self.socket = context.wrap_socket(self.socket, server_hostname=server_name or address[0])
            elif server_name:
                raise MigrationError(label + " TLS server name requires TLS")
            self.reader = self.socket.makefile("rb")
            if password is not None:
                command = [b"AUTH", password] if not username else [b"AUTH", username.encode("utf-8"), password]
                if self.call(*command) != b"OK":
                    raise MigrationError(label + " AUTH did not return OK")
            if self.call(b"PING") != b"PONG":
                raise MigrationError(label + " PING did not return PONG")
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.reader is not None:
            self.reader.close()
        if self.socket is not None:
            self.socket.close()

    def _budget(self, count):
        self.remaining -= count
        if self.remaining < 0:
            raise MigrationError(self.label + " oversized reply/page exceeds the 8 MiB memory bound; reduce --page-size")
        remaining_time = self.deadline - time.monotonic()
        if remaining_time <= 0:
            raise MigrationError(self.label + " command deadline exceeded")
        self.socket.settimeout(remaining_time)

    def _line(self):
        line = bytearray()
        while len(line) <= 4096:
            self._budget(0)
            byte = self.reader.read1(1)
            if not byte:
                raise MigrationError(self.label + " disconnected during RESP header")
            self._budget(1)
            line.extend(byte)
            if line.endswith(b"\r\n"):
                return bytes(line[:-2])
        raise MigrationError(self.label + " invalid/oversized RESP header")

    def _exact(self, count):
        self._budget(count)
        # Read in bounded chunks so slow trickle traffic cannot reset the overall deadline.
        chunks = []
        remaining = count
        while remaining:
            self._budget(0)
            chunk = self.reader.read1(min(remaining, 65536))
            if not chunk:
                raise MigrationError(self.label + " disconnected during RESP value")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def _reply(self, depth=0):
        self.reply_nodes += 1
        if self.reply_nodes > 32768:
            raise MigrationError(self.label + " oversized reply exceeds 32768 decoded nodes")
        if depth > 8:
            raise MigrationError(self.label + " RESP nesting exceeds bound")
        line = self._line()
        kind, value = line[:1], line[1:]
        if kind == b"+":
            return value
        if kind == b"-":
            # Never echo arbitrary server text: it can contain keys or credentials.
            return ServerError(self.label + " server rejected command")
        if kind not in (b":", b"$", b"*"):
            raise MigrationError(self.label + " unsupported RESP reply")
        try:
            length = int(value)
        except ValueError:
            raise MigrationError(self.label + " invalid RESP length/integer") from None
        if kind == b":":
            return length
        if length == -1:
            return None
        if length < 0:
            raise MigrationError(self.label + " invalid negative RESP length")
        if kind == b"$":
            if length > MAX_COMMAND:
                raise MigrationError(self.label + " oversized value exceeds 8 MiB; nothing is skipped/truncated")
            data = self._exact(length + 2)
            if not data.endswith(b"\r\n"):
                raise MigrationError(self.label + " invalid RESP bulk terminator")
            return data[:-2]
        if length > MAX_ARRAY:
            raise MigrationError(self.label + " oversized array/page exceeds 16384 entries; reduce --page-size")
        return [self._reply(depth + 1) for _ in range(length)]

    def call(self, *parts):
        wire = encode(parts)
        self.deadline = time.monotonic() + self.timeout
        self.remaining = MAX_REPLY
        self.reply_nodes = 0
        self.socket.settimeout(self.timeout)
        self.socket.sendall(wire)
        result = self._reply()
        if isinstance(result, ServerError):
            command = argument(parts[0]).decode("ascii", "replace")
            raise ServerError(self.label + " rejected " + command + "; check command support, ACL permissions and server logs")
        return result


def array(value, label):
    if not isinstance(value, list):
        raise MigrationError("invalid " + label + " response")
    return value


def integer(value, label):
    if not isinstance(value, int):
        raise MigrationError("invalid " + label + " integer response")
    return value


def page(client, command, *prefix, count):
    cursor = b"0"
    while True:
        result = array(client.call(command, *prefix, cursor, b"COUNT", count), "cursor")
        if len(result) != 2 or not isinstance(result[0], bytes):
            raise MigrationError("invalid cursor response")
        cursor, values = result
        yield array(values, "cursor page")
        if cursor == b"0":
            return


def supports(client, command):
    response = array(client.call(b"COMMAND", b"INFO", command), "COMMAND INFO")
    if len(response) != 1:
        raise MigrationError(client.label + " invalid COMMAND INFO response")
    return response[0] is not None


class Copier:
    def __init__(self, source, target, count, stats, field_ttl):
        self.source, self.target = source, target
        self.count, self.stats, self.field_ttl = count, stats, field_ttl
        self.key = None

    def expiry(self, key):
        expiry = integer(self.source.call(b"PEXPIRETIME", key), "PEXPIRETIME")
        if expiry < -2:
            raise MigrationError("invalid source key expiry")
        return expiry

    def mutate(self, key, expiry, *commands):
        commands = list(commands)
        if expiry >= 0:
            commands.append((b"PEXPIREAT", key, expiry))
        # Pre-encode EVERY command before opening MULTI; oversize cannot leave a queued prefix.
        for command in commands:
            encode(command)
        if self.target.call(b"MULTI") != b"OK":
            raise MigrationError("target MULTI did not return OK")
        try:
            for command in commands:
                if self.target.call(*command) != b"QUEUED":
                    raise MigrationError("target transaction did not queue command")
        except BaseException:
            # No mutation is applied until EXEC. Closing the socket discards queued work.
            self.target.close()
            raise
        self.stats["target_touched"] = True  # EXEC reply may be lost; report uncertainty conservatively.
        results = array(self.target.call(b"EXEC"), "EXEC")
        self.stats["write_commands"] += len(commands)
        if len(results) != len(commands) or any(isinstance(result, ServerError) for result in results):
            raise MigrationError("target transaction contained a command error; partial target retained")

    def remove_created(self, key):
        self.stats["target_touched"] = True
        integer(self.target.call(b"DEL", key), "DEL")
        self.stats["write_commands"] += 1

    def copy(self, key):
        self.key = hashlib.sha256(key).hexdigest()[:16]
        expiry = self.expiry(key)
        kind = self.source.call(b"TYPE", key)
        if expiry == -2 or kind == b"none":
            self.stats["expired_keys"] += 1
            return
        if kind not in (b"string", b"list", b"set", b"hash", b"zset"):
            safe_type = kind.decode("ascii", "replace")[:64] if isinstance(kind, bytes) else "invalid"
            raise MigrationError("unsupported source type " + safe_type + "; no key was silently skipped")
        if integer(self.target.call(b"EXISTS", key), "EXISTS"):
            raise MigrationError("target key collision; migration never replaces existing keys")
        if kind == b"string":
            value = self.source.call(b"GET", key)
            if value is not None:
                if not isinstance(value, bytes):
                    raise MigrationError("invalid source string response")
                self.mutate(key, expiry, (b"SET", key, value, b"NX"))
                self.stats["written_items"] += 1
        elif kind == b"list":
            # One item per read gives a hard bounded page even for maximum-sized list items.
            index = 0
            while True:
                values = array(self.source.call(b"LRANGE", key, index, index), "LRANGE")
                if not values:
                    break
                if len(values) != 1 or not isinstance(values[0], bytes):
                    raise MigrationError("invalid LRANGE response")
                self.mutate(key, expiry, (b"RPUSH", key, values[0]))
                self.stats["written_items"] += 1
                index += 1
        else:
            command = {b"set": b"SSCAN", b"hash": b"HSCAN", b"zset": b"ZSCAN"}[kind]
            for values in page(self.source, command, key, count=self.count):
                if not all(isinstance(value, bytes) for value in values):
                    raise MigrationError("invalid binary cursor member")
                if kind == b"set":
                    for member in values:
                        self.mutate(key, expiry, (b"SADD", key, member))
                        self.stats["written_items"] += 1
                    continue
                if len(values) % 2:
                    raise MigrationError("invalid tuple cursor page")
                for index in range(0, len(values), 2):
                    member, value = values[index:index + 2]
                    if kind == b"zset":
                        self.mutate(key, expiry, (b"ZADD", key, value, member))
                    else:
                        field_expiry = -1
                        if self.field_ttl:
                            response = array(self.source.call(b"HPEXPIRETIME", key, b"FIELDS", 1, member), "HPEXPIRETIME")
                            if len(response) != 1:
                                raise MigrationError("invalid field expiry response")
                            field_expiry = integer(response[0], "HPEXPIRETIME")
                            if field_expiry == -2:
                                self.stats["expired_fields"] += 1
                                continue
                            if field_expiry < -2:
                                raise MigrationError("invalid source field expiry")
                        commands = [(b"HSET", key, member, value)]
                        if field_expiry >= 0:
                            commands.append((b"HPEXPIREAT", key, field_expiry, b"FIELDS", 1, member))
                        self.mutate(key, expiry, *commands)
                    self.stats["written_items"] += 1
        # A key may naturally expire between metadata, pages, or the final write.
        # Remove only this invocation's newly-created key; never a preexisting key.
        final_expiry = self.expiry(key)
        if final_expiry == -2:
            self.remove_created(key)
            self.stats["expired_keys"] += 1
            return
        if final_expiry != expiry:
            raise MigrationError("source expiry changed: source was not quiesced")
        if integer(self.target.call(b"EXISTS", key), "EXISTS"):
            self.stats["copied_keys"] += 1
        else:
            self.stats["expired_keys"] += 1


def options():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=endpoint, required=True, metavar="HOST:PORT")
    parser.add_argument("--target", type=endpoint, required=True, metavar="HOST:PORT")
    parser.add_argument("--source-quiesced", action="store_true", required=True,
                        help="acknowledge source has no writers; target is empty and exclusive")
    for label in ("source", "target"):
        parser.add_argument("--" + label + "-username")
        parser.add_argument("--" + label + "-password-file")
        parser.add_argument("--" + label + "-tls", action="store_true", help="TLS using system certificate trust")
        parser.add_argument("--" + label + "-tls-ca", help="TLS using this CA certificate file")
        parser.add_argument("--" + label + "-tls-server-name", help="certificate/SNI name; never disables verification")
    parser.add_argument("--timeout", type=float, default=10.0, help="finite deadline per command, seconds (default: 10)")
    parser.add_argument("--page-size", type=int, default=32, help="SCAN-family COUNT hint, 1..128 (default: 32)")
    args = parser.parse_args()
    if not 0 < args.timeout <= 300 or not 1 <= args.page_size <= 128:
        parser.error("timeout must be in (0,300] and page-size in [1,128]")
    if args.source == args.target:
        parser.error("source and target endpoints must differ")
    return args


def main():
    args = options()
    stats = {"status": "starting", "scanned_keys": 0, "unique_keys": 0, "copied_keys": 0,
             "duplicate_scan_keys": 0, "expired_keys": 0, "expired_fields": 0, "written_items": 0,
             "write_commands": 0, "errors": 0, "target_touched": False, "partial_target": False}
    clients = []
    copier = None
    started = time.monotonic()
    try:
        for label in ("source", "target"):
            clients.append(Client(label, getattr(args, label), getattr(args, label + "_username"),
                                  getattr(args, label + "_password_file"), getattr(args, label + "_tls"),
                                  getattr(args, label + "_tls_ca"), getattr(args, label + "_tls_server_name"), args.timeout))
        source, target = clients
        if integer(target.call(b"DBSIZE"), "DBSIZE") != 0:
            raise MigrationError("target is not empty; no destructive replace mode exists")
        if not supports(source, b"PEXPIRETIME"):
            raise MigrationError("source must support PEXPIRETIME for exact absolute TTL migration")
        field_ttl = supports(source, b"HPEXPIRETIME")
        if field_ttl and not supports(target, b"HPEXPIREAT"):
            raise MigrationError("target does not support absolute hash field expiry")
        stats["source_field_ttl_supported"] = field_ttl
        copier = Copier(source, target, args.page_size, stats, field_ttl)
        with tempfile.TemporaryDirectory(prefix="ffkv-migrate-") as temporary:
            with sqlite3.connect(str(Path(temporary) / "seen.sqlite")) as seen:
                seen.execute("PRAGMA cache_size=-2048")
                seen.execute("CREATE TABLE seen (key BLOB PRIMARY KEY) WITHOUT ROWID")
                for keys in page(source, b"SCAN", count=args.page_size):
                    for key in keys:
                        stats["scanned_keys"] += 1
                        if not isinstance(key, bytes):
                            raise MigrationError("source SCAN returned a nonbinary key")
                        inserted = seen.execute("INSERT OR IGNORE INTO seen(key) VALUES (?)", (key,)).rowcount
                        if not inserted:
                            stats["duplicate_scan_keys"] += 1
                            continue
                        stats["unique_keys"] += 1
                        copier.copy(key)
                    seen.commit()
        stats["status"] = "complete"
        return_code = 0
    except (MigrationError, OSError, sqlite3.Error, ValueError, KeyboardInterrupt) as error:
        stats["errors"] += 1
        stats["partial_target"] = stats["target_touched"]
        stats["status"] = "failed_partial_target" if stats["partial_target"] else "failed_before_copy"
        if isinstance(error, MigrationError):
            stats["error"] = str(error)
        elif isinstance(error, KeyboardInterrupt):
            stats["error"] = "interrupted; a lost mutation reply has an unknown outcome"
        else:
            stats["error"] = type(error).__name__ + ": connection, TLS, credential file or temporary storage failure"
        if copier is not None and copier.key is not None:
            stats["key_sha256_prefix"] = copier.key
        return_code = 2
    finally:
        for client in clients:
            client.close()
    stats["elapsed_seconds"] = round(time.monotonic() - started, 6)
    print(json.dumps(stats, sort_keys=True))
    return return_code


if __name__ == "__main__":
    sys.exit(main())
