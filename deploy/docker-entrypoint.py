#!/usr/bin/env python3
"""Container entrypoint for the published ffkv image.

With no server flags that choose the listener or its credentials, the server
binds 0.0.0.0 with TLS and an ACL generated on first start. A non-loopback
listener requires both, so the image creates them rather than weakening that
rule.
State lives in the data volume under ``/var/lib/ffkv/docker``:

* ``admin-password`` / ``health-password``: random unless ``FFKV_PASSWORD`` or
  ``FFKV_PASSWORD_FILE`` supplies the admin password.
* ``users.acl``: rewritten on every start from those passwords.
* ``tls.crt`` / ``tls.key`` / ``ca.crt``: a self-signed certificate for
  localhost, 127.0.0.1 and any ``FFKV_TLS_SANS`` entries. It is
  regenerated when those names change or it expires within 30 days.

Passing ``--bind``, ``--acl-file``, ``--tls-cert`` or ``--tls-key`` runs ffkv
with exactly the given arguments (the Compose workflow does this). A first
argument that is not a flag runs that program instead, e.g. ``ffkv-cli`` or
``sh``. Passwords are never printed or passed on a command line.
"""
import hashlib
import ipaddress
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

DATA = Path("/var/lib/ffkv")
STATE = DATA / "docker"
EXPLICIT = ("--bind", "--acl-file", "--tls-cert", "--tls-key")


def log(message):
    print("ffkv-entrypoint: " + message, file=sys.stderr, flush=True)


def private_write(path, content):
    temporary = path.with_name(path.name + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(content)
    os.replace(temporary, path)


def admin_password():
    path = STATE / "admin-password"
    supplied = os.environ.get("FFKV_PASSWORD")
    if os.environ.get("FFKV_PASSWORD_FILE"):
        supplied = Path(os.environ["FFKV_PASSWORD_FILE"]).read_text().rstrip("\n")
    if supplied:
        if any(char.isspace() for char in supplied):
            raise SystemExit("ffkv-entrypoint: FFKV_PASSWORD must not contain whitespace")
        private_write(path, supplied + "\n")
        return supplied, False
    if path.exists():
        return path.read_text().rstrip("\n"), False
    password = secrets.token_urlsafe(32)
    private_write(path, password + "\n")
    return password, True


def stored_password(name):
    path = STATE / name
    if not path.exists():
        private_write(path, secrets.token_urlsafe(32) + "\n")
    return path.read_text().rstrip("\n")


def subject_names():
    # Not the container hostname: it changes on every `docker run`, which would
    # rotate the certificate clients already trust. Use FFKV_TLS_SANS instead.
    names = ["localhost", "ffkv", "127.0.0.1", "::1"]
    names += [entry.strip() for entry in os.environ.get("FFKV_TLS_SANS", "").split(",")]
    unique = []
    for name in names:
        if name and name not in unique:
            unique.append(name)
    entries = []
    for name in unique:
        try:
            entries.append("IP:" + str(ipaddress.ip_address(name)))
        except ValueError:
            entries.append("DNS:" + name)
    return ",".join(entries)


def certificate_current(sans):
    crt, key, recorded = STATE / "tls.crt", STATE / "tls.key", STATE / "tls.sans"
    if not (crt.exists() and key.exists() and recorded.exists()):
        return False
    if recorded.read_text().strip() != sans:
        return False
    # Valid for at least another 30 days.
    return subprocess.run(["openssl", "x509", "-checkend", "2592000", "-noout", "-in", str(crt)],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def ensure_certificate():
    sans = subject_names()
    if certificate_current(sans):
        return False
    config = STATE / "certificate-request.conf"
    private_write(config, "[req]\nprompt=no\ndistinguished_name=dn\nx509_extensions=extensions\n"
                  "[dn]\nCN=ffkv container (self-signed)\n[extensions]\n"
                  "subjectAltName=" + sans + "\n"
                  "subjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid:always\n"
                  "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n"
                  "extendedKeyUsage=serverAuth\n")
    try:
        completed = subprocess.run(
            ["openssl", "req", "-x509", "-newkey", "rsa:3072", "-sha256", "-nodes", "-days", "825",
             "-config", str(config), "-keyout", str(STATE / "tls.key.tmp"), "-out", str(STATE / "tls.crt.tmp")],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, timeout=120)
        if completed.returncode:
            raise SystemExit("ffkv-entrypoint: certificate generation failed: " + completed.stderr.strip())
    finally:
        config.unlink(missing_ok=True)
    for name in ("tls.key", "tls.crt"):
        os.chmod(STATE / (name + ".tmp"), 0o600)
        os.replace(STATE / (name + ".tmp"), STATE / name)
    private_write(STATE / "ca.crt", (STATE / "tls.crt").read_text())
    private_write(STATE / "tls.sans", sans + "\n")
    return True


def automatic(extra):
    port = os.environ.get("FFKV_PORT", "6379")
    if not port.isdigit() or not 1 <= int(port) <= 65535:
        raise SystemExit("ffkv-entrypoint: FFKV_PORT must be a TCP port")
    STATE.mkdir(mode=0o700, parents=True, exist_ok=True)
    password, generated = admin_password()
    health = stored_password("health-password")
    digest = lambda text: hashlib.sha256(text.encode()).hexdigest()
    private_write(STATE / "users.acl", "user default off\n"
                  "user admin on #" + digest(password) + " ~* &* +@all\n"
                  "user health on #" + digest(health) + " +ping\n")
    if ensure_certificate():
        log("generated a self-signed TLS certificate for " + (STATE / "tls.sans").read_text().strip())
    private_write(STATE / "client.json", json.dumps({"port": int(port), "ca": str(STATE / "ca.crt"),
                                                     "server_name": "localhost"}) + "\n")
    if generated:
        log("generated an admin password; read it with: docker exec <container> cat " + str(STATE / "admin-password"))
    log("TLS + ACL listener on 0.0.0.0:" + port + "; inside the container just run: ffkv-cli PING")
    durability = os.environ.get("FFKV_DURABILITY", "strict")
    return ["ffkv", "--bind", "0.0.0.0:" + port, "--data-dir", str(DATA), "--durability", durability,
            "--acl-file", str(STATE / "users.acl"), "--tls-cert", str(STATE / "tls.crt"),
            "--tls-key", str(STATE / "tls.key"), *extra]


def main(arguments):
    if arguments and not arguments[0].startswith("-"):
        command = arguments
    elif any(argument.split("=", 1)[0] in EXPLICIT for argument in arguments):
        command = ["ffkv", *arguments]
    else:
        command = automatic(arguments)
    os.execvp(command[0], command)


if __name__ == "__main__":
    main(sys.argv[1:])
