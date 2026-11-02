#!/usr/bin/env python3
"""Finite authenticated TLS RESP PING; never prints credentials or server replies."""
import argparse
import socket
import ssl
import sys
from pathlib import Path


def frame(parts):
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n" % len(p) + p + b"\r\n" for p in parts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--username", default="health")
    parser.add_argument("--password-file", required=True)
    parser.add_argument("--tls-ca", required=True)
    parser.add_argument("--tls-server-name")
    parser.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args()
    if not 0 < args.timeout <= 60 or not 1 <= args.port <= 65535:
        parser.error("invalid port or timeout")
    try:
        with Path(args.password_file).open("rb") as source:
            password = source.read(16386)
        if password.endswith(b"\n"):
            password = password[:-1]
        if not password or len(password) > 16384:
            raise ValueError("invalid password file")
        context = ssl.create_default_context(cafile=args.tls_ca)
        with socket.create_connection((args.host, args.port), timeout=args.timeout) as raw:
            with context.wrap_socket(raw, server_hostname=args.tls_server_name or args.host) as conn:
                conn.settimeout(args.timeout)
                with conn.makefile("rb") as reader:
                    conn.sendall(frame([b"AUTH", args.username.encode(), password]))
                    if reader.readline(1024) != b"+OK\r\n":
                        raise ValueError("authentication rejected")
                    conn.sendall(frame([b"PING"]))
                    if reader.readline(1024) != b"+PONG\r\n":
                        raise ValueError("PING rejected")
        return 0
    except (OSError, ValueError):
        print("ffkv authenticated TLS healthcheck failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
