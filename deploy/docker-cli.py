#!/usr/bin/env python3
"""``ffkv-cli`` inside the published image.

With no connection options, connects to the local server using the TLS
certificate and admin password the entrypoint generated, so
``docker exec <container> ffkv-cli PING`` just works. Any explicit connection
option or ``FFKV_URL`` passes through to the real client unchanged.
"""
import json
import os
import sys
from pathlib import Path

CLIENT = "/usr/local/libexec/ffkv/ffkv-cli"
STATE = Path("/var/lib/ffkv/docker")
CONNECTION = {"-h", "--host", "-p", "--port", "-u", "--uri", "--user", "-a", "--pass",
              "--askpass", "--tls", "--cacert", "--sni", "--cert", "--key"}


def explicit(arguments):
    for argument in arguments:
        if not argument.startswith("-") or argument == "--":
            return False  # options precede COMMAND
        if argument.split("=", 1)[0] in CONNECTION:
            return True
    return False


def main(arguments):
    environment = dict(os.environ)
    command = [CLIENT, *arguments]
    try:
        if not explicit(arguments) and "FFKV_URL" not in environment:
            settings = json.loads((STATE / "client.json").read_text())
            environment["FFKVCLI_AUTH"] = (STATE / "admin-password").read_text().rstrip("\n")
            command = [CLIENT, "--tls", "--cacert", settings["ca"], "--sni", settings["server_name"],
                       "-p", str(settings["port"]), "--user", "admin", *arguments]
    except (OSError, ValueError, KeyError):
        pass  # not started by the image entrypoint: behave like the plain client
    os.execve(CLIENT, command, environment)


if __name__ == "__main__":
    main(sys.argv[1:])
