#!/usr/bin/env python3
"""Require passed Redis 8.10 scenarios AND positive observed wire invocations.

Full runs include native replication and cluster fixtures. Scoped runs never
claim catalog completion. Optional evidence reports contain only canonical
command/status/scenario and are bounded to one successful scenario per command.
Only catalog containers HIMPORT, JSON.DEBUG and HOTKEYS share a successful child
invocation's coverage. COMMAND and every other standalone entry remain distinct.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from evidence import CATALOG

ROOT = Path(__file__).resolve().parents[2]
BDD = ROOT / "tests/bdd"


def command_tag(name):
    return "command_" + re.sub(r"[^A-Z0-9]", "_", name.upper())


def inventory():
    from behave.parser import parse_file
    catalog = CATALOG
    tagged = set()
    for path in sorted((BDD / "features").glob("*.feature")):
        feature = parse_file(str(path))
        for scenario in feature.scenarios:
            tagged.update(str(tag) for tag in feature.tags)
            tagged.update(str(tag) for tag in scenario.tags)
    missing = [row["name"] for row in catalog if command_tag(row["name"]) not in tagged]
    return catalog, missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", action="store_true", help="Only audit scenario tags; does not claim behavioral support")
    parser.add_argument("--feature", action="append", help="Run an explicitly scoped feature, without claiming catalog completion")
    parser.add_argument("--tags", help="Run an explicitly scoped tag expression")
    parser.add_argument("--server", default=str(ROOT / "target/debug/ffkv"))
    parser.add_argument("--client", default=str(ROOT / "target/debug/ffkv-cli"))
    parser.add_argument("--mcp", help="MCP sidecar binary; defaults to ffkv-mcp beside --server")
    parser.add_argument("--evidence-report", type=Path, help="Save bounded positive canonical wire evidence (no argv)")
    args = parser.parse_args()
    catalog, missing = inventory()
    print(f"Redis 8.10 catalog: {len(catalog)} entries; scenario tags: {len(catalog) - len(missing)} covered; {len(missing)} missing", flush=True)
    if missing:
        print("Missing scenario tags: " + ", ".join(missing), flush=True)
    if args.inventory:
        return 2 if missing else 0
    scoped = bool(args.feature or args.tags)
    if missing and not scoped:
        return 2
    with tempfile.TemporaryDirectory(prefix="ffkv-bdd-report-") as directory:
        report = Path(directory) / "results.json"
        evidence = Path(directory) / "wire-evidence.json"
        command = [sys.executable, "-m", "behave", *(args.feature or [str(BDD / "features")]),
                   "-D", "server=" + str(Path(args.server).resolve()),
                   "-D", "client=" + str(Path(args.client).resolve()),
                   "-D", "mcp=" + str(Path(args.mcp).resolve() if args.mcp else Path(args.server).resolve().with_name("ffkv-mcp")),
                   "-D", "evidence_report=" + str(evidence),
                   "--format", "pretty", "--outfile", "-", "--format", "json", "--outfile", str(report)]
        if args.tags:
            command.extend(["--tags", args.tags])
        result = subprocess.run(command, cwd=ROOT)
        records = json.loads(evidence.read_text()) if evidence.exists() else []
        assert len(records) <= len(catalog), "BDD wire evidence record bound exceeded"
        names = {row["name"] for row in catalog}
        assert all(set(row) == {"command", "status", "scenario"} and row["command"] in names
                   and row["status"] == "positive" and isinstance(row["scenario"], str)
                   and len(row["scenario"]) <= 2048 for row in records), "Invalid BDD wire evidence"
        executed = {row["command"] for row in records}
        assert len(executed) == len(records), "Duplicate BDD wire evidence"
        print(f"Positive canonical wire coverage in passed scenarios: {len(executed)}/{len(catalog)}", flush=True)
        print("Container coverage: HIMPORT, JSON.DEBUG, HOTKEYS also count their positive catalog child invocations; COMMAND requires its own invocation.", flush=True)
        if args.evidence_report:
            args.evidence_report.write_text(json.dumps(records, indent=2) + "\n")
        if result.returncode:
            return result.returncode
        if scoped:
            print("Scoped BDD run passed; this is not full-catalog verification.")
            return 0
        passed = set()
        for feature in json.loads(report.read_text()):
            for scenario in feature.get("elements", []):
                steps = scenario.get("steps", [])
                if steps and all(step.get("result", {}).get("status") == "passed" for step in steps):
                    passed.update(feature.get("tags", []))
                    passed.update(scenario.get("tags", []))
        unverified = [row["name"] for row in catalog if command_tag(row["name"]) not in passed]
        unexecuted = [row["name"] for row in catalog if row["name"] not in executed]
        if unverified:
            print("No passing tagged scenario for: " + ", ".join(unverified))
        if unexecuted:
            print("No positive observed invocation in a passed scenario for: " + ", ".join(unexecuted))
        if unverified or unexecuted:
            return 2
        print(f"All {len(catalog)} catalog entries have passing tagged scenarios and positive observed invocations.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
