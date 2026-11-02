#!/usr/bin/env python3
"""Create a NEW private development configuration; requires Python 3 and OpenSSL.

Replace development certificates and local files with your production PKI and
secret manager. This program refuses existing destinations, including empty ones.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys


def private_file(path, content):
    data = content.encode() if isinstance(content, str) else content
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as output:
        output.write(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="new configuration directory, never an existing path")
    args = parser.parse_args()
    uid = os.getuid() if hasattr(os, "getuid") else 10001
    gid = os.getgid() if hasattr(os, "getgid") else 10001
    if uid == 0:
        parser.error("run bootstrap as the nonroot owner that will run the Compose container")
    openssl = shutil.which("openssl")
    if not openssl:
        parser.error("openssl is required to generate development TLS certificates")
    directory = args.directory.absolute()
    if any(char in str(directory) for char in "\r\n'\\"):
        parser.error("directory must not contain quotes, backslashes or newlines (Compose env-file safety)")
    try:
        # mkdir, rather than exists()+mkdir, is the no-overwrite boundary.
        directory.mkdir(mode=0o700, parents=False, exist_ok=False)
        os.chmod(directory, 0o700)
        (directory / "data").mkdir(mode=0o700)
        passwords = {name: secrets.token_urlsafe(48) for name in ("admin", "health")}
        tokens = {role: secrets.token_urlsafe(48) for role in ("admin", "viewer")}
        for name, password in passwords.items():
            private_file(directory / (name + "-password"), password + "\n")
        for role, token in tokens.items():
            private_file(directory / ("console-" + role + "-token"), token + "\n")
        digest = lambda text: hashlib.sha256(text.encode()).hexdigest()
        private_file(directory / "users.acl", "user default off\n"
                     + "user admin on #" + digest(passwords["admin"]) + " ~* +@all\n"
                     + "user health on #" + digest(passwords["health"]) + " +ping\n")
        private_file(directory / "management-tokens.json", json.dumps({"tokens": [
            {"name": "local-" + role, "role": role, "sha256": digest(token)}
            for role, token in tokens.items()]}, indent=2) + "\n")
        config = directory / "certificate-request.conf"
        private_file(config, "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=extensions\n"
                     "[dn]\nCN=ffkv development only\n[extensions]\n"
                     "subjectAltName=DNS:localhost,DNS:ffkv,IP:127.0.0.1\n"
                     "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n"
                     "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n"
                     "extendedKeyUsage=serverAuth\n")
        try:
            # No passwords or tokens are passed in arguments, environment or output.
            completed = subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:3072", "-sha256",
                                        "-nodes", "-days", "30", "-config", str(config),
                                        "-keyout", str(directory / "tls.key"),
                                        "-out", str(directory / "tls.crt")],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
            if completed.returncode:
                raise ValueError("OpenSSL certificate generation failed")
        finally:
            config.unlink()
        for name in ("tls.key", "tls.crt"):
            os.chmod(directory / name, 0o600)
        private_file(directory / "ca.crt", (directory / "tls.crt").read_bytes())
        private_file(directory / "compose.env", "FFKV_CONFIG_DIR='" + str(directory) + "'\n"
                     + f"FFKV_UID={uid}\nFFKV_GID={gid}\n")
        print("Created private development configuration: " + str(directory))
        print("Compose env-file: " + str(directory / "compose.env"))
        print("Credentials are file-only; development certificate expires in 30 days.")
        return 0
    except FileExistsError:
        print("Refusing an existing destination; choose a NEW directory.", file=sys.stderr)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        print("Bootstrap failed; any newly created directory is incomplete. Choose a NEW directory; no existing paths were overwritten.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
