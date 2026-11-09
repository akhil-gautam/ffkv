#!/usr/bin/env python3
"""Keep every ffkv version string on one Semantic Versioning 2.0.0 value.

Cargo.toml's package version is the source of truth. The other files below
repeat it because their own tooling reads them (Cargo.lock, npm, Helm).

  python3 scripts/version.py            print the version
  python3 scripts/version.py check      fail if any file disagrees
  python3 scripts/version.py check --tag v1.2.3
                                        also require the tag to be v<version>
  python3 scripts/version.py set 1.2.3  rewrite every file to 1.2.3
  python3 scripts/version.py notes 1.2.3
                                        print that version's CHANGELOG section
"""

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The regular expression published at https://semver.org/ (named groups).
SEMVER = re.compile(
    r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)"
    r"(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?"
    r"(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)

# (file, pattern) pairs; group 1 is the version. Each must match exactly once.
TEXT_FIELDS = [
    ("Cargo.toml", r'(?m)^\[package\]\nname = "ffkv"\nversion = "([^"]+)"'),
    ("Cargo.lock", r'(?m)^name = "ffkv"\nversion = "([^"]+)"'),
    ("deploy/helm/ffkv/Chart.yaml", r"(?m)^version: (\S+)$"),
    ("deploy/helm/ffkv/Chart.yaml", r"(?m)^appVersion: '([^']+)'$"),
    ("deploy/helm/ffkv/values.yaml", r"(?m)^  tag: '([^']+)'$"),
]

NPM_PACKAGES = ["apps/docs", "apps/console"]


def fail(message):
    print(f"version: {message}", file=sys.stderr)
    sys.exit(1)


def text_match(relative, pattern):
    text = (ROOT / relative).read_text()
    matches = list(re.finditer(pattern, text))
    if len(matches) != 1:
        fail(f"{relative}: expected one version field, found {len(matches)}")
    return text, matches[0]


def npm_documents(directory):
    """Yield (path, parsed JSON, original text) for package.json and its lockfile."""
    for name in ("package.json", "package-lock.json"):
        path = ROOT / directory / name
        text = path.read_text()
        data = json.loads(text)
        if dump_npm(data) != text:
            fail(f"{path.relative_to(ROOT)}: not in npm's canonical format; refusing to rewrite it")
        yield path, data, text


def dump_npm(data):
    return json.dumps(data, indent=2, ensure_ascii=False) + "\n"


def npm_versions(path, data):
    versions = {f"{path.name} version": data.get("version")}
    if path.name == "package-lock.json":
        versions["package-lock.json packages[''].version"] = data["packages"][""].get("version")
    return versions


def current():
    return text_match(*TEXT_FIELDS[0])[1].group(1)


def found_versions():
    found = []
    for relative, pattern in TEXT_FIELDS:
        found.append((relative, text_match(relative, pattern)[1].group(1)))
    for directory in NPM_PACKAGES:
        for path, data, _ in npm_documents(directory):
            for label, value in npm_versions(path, data).items():
                found.append((f"{directory}/{label}", value))
    return found


def check(tag):
    version = current()
    if not SEMVER.match(version):
        fail(f"Cargo.toml version {version!r} is not a valid SemVer version")
    mismatched = [(where, value) for where, value in found_versions() if value != version]
    for where, value in mismatched:
        print(f"version: {where} is {value!r}, expected {version!r}", file=sys.stderr)
    if mismatched:
        sys.exit(1)
    if tag is not None and tag != f"v{version}":
        fail(f"tag {tag!r} does not match Cargo.toml version; expected 'v{version}'")
    print(version)


def set_version(version):
    if not SEMVER.match(version):
        fail(f"{version!r} is not a valid SemVer version (see https://semver.org/)")
    rewrites = {}
    for relative, pattern in TEXT_FIELDS:
        text = rewrites.get(relative) or (ROOT / relative).read_text()
        matches = list(re.finditer(pattern, text))
        if len(matches) != 1:
            fail(f"{relative}: expected one version field, found {len(matches)}")
        start, end = matches[0].span(1)
        rewrites[relative] = text[:start] + version + text[end:]
    for directory in NPM_PACKAGES:
        for path, data, _ in npm_documents(directory):
            data["version"] = version
            if path.name == "package-lock.json":
                data["packages"][""]["version"] = version
            rewrites[str(path.relative_to(ROOT))] = dump_npm(data)
    for relative, text in rewrites.items():
        (ROOT / relative).write_text(text)
    check(None)


def notes(version):
    changelog = (ROOT / "CHANGELOG.md").read_text()
    heading = re.compile(rf"(?m)^## \[{re.escape(version)}\][^\n]*\n")
    found = heading.search(changelog)
    if not found:
        fail(f"CHANGELOG.md has no '## [{version}]' section")
    following = re.compile(r"(?m)^## \[").search(changelog, found.end())
    body = changelog[found.end(): following.start() if following else len(changelog)]
    body = re.sub(r"(?m)^\[[^\]]+\]: \S+\n?", "", body).strip()
    if not body:
        fail(f"CHANGELOG.md section {version} is empty")
    print(body)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command")
    check_parser = commands.add_parser("check")
    check_parser.add_argument("--tag")
    commands.add_parser("set").add_argument("version")
    commands.add_parser("notes").add_argument("version")
    args = parser.parse_args()
    if args.command == "check":
        check(args.tag)
    elif args.command == "set":
        set_version(args.version)
    elif args.command == "notes":
        notes(args.version)
    else:
        print(current())


if __name__ == "__main__":
    main()
