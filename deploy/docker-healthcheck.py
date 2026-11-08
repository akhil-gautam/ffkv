#!/usr/bin/env python3
"""Image HEALTHCHECK: an authenticated TLS PING when the entrypoint generated
credentials, otherwise a plain TCP connect to the listener port."""
import json
import os
import socket
import sys
from pathlib import Path

STATE = Path("/var/lib/ffkv/docker")


def main():
    try:
        settings = json.loads((STATE / "client.json").read_text())
    except (OSError, ValueError):
        port = int(os.environ.get("FFKV_PORT", "6379"))
        try:
            socket.create_connection(("127.0.0.1", port), timeout=3).close()
            return 0
        except OSError:
            return 1
    probe = ["python3", "/usr/local/bin/ffkv-probe", "--host", "127.0.0.1", "--port", str(settings["port"]),
             "--username", "health", "--password-file", str(STATE / "health-password"),
             "--tls-ca", settings["ca"], "--tls-server-name", settings["server_name"]]
    os.execvp(probe[0], probe)


if __name__ == "__main__":
    sys.exit(main())
