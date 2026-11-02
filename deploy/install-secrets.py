#!/usr/bin/env python3
"""Copy projected Kubernetes Secret files to private regular files for O_NOFOLLOW policies."""
import os
from pathlib import Path
import shutil
import sys
import tempfile

NAMES = ("users.acl", "management-tokens.json", "health-password", "tls.crt", "tls.key", "ca.crt")


def main():
    source, destination = Path("/projected"), Path("/runtime/config")
    temporary = None
    try:
        content = {}
        for name in NAMES:
            with (source / name).open("rb") as handle:
                content[name] = handle.read(65537)
            if not content[name] or len(content[name]) > 65536:
                raise ValueError("invalid secret file size")
        if destination.exists() or destination.is_symlink():
            if destination.is_symlink() or set(p.name for p in destination.iterdir()) != set(NAMES):
                raise ValueError("unexpected destination")
            for name in NAMES:
                path = destination / name
                if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077 or path.read_bytes() != content[name]:
                    raise ValueError("secret destination differs")
            return 0
        temporary = Path(tempfile.mkdtemp(prefix=".config-", dir=destination.parent))
        os.chmod(temporary, 0o700)
        for name in NAMES:
            descriptor = os.open(temporary / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(content[name])
        os.rename(temporary, destination)
        temporary = None
        return 0
    except (OSError, ValueError):
        print("Secret installation failed; check external Secret keys and private volume ownership", file=sys.stderr)
        return 1
    finally:
        if temporary is not None:
            shutil.rmtree(temporary)


if __name__ == "__main__":
    sys.exit(main())
