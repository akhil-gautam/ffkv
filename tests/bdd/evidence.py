"""Positive wire evidence: canonical identities only, never command arguments.

The full runner requires both passed scenario tags and these observed successes.
A send, an error, an unsolicited push, or a replication handshake alone earns no
coverage. Reports contain at most one passed scenario per catalog identity.
HIMPORT, JSON.DEBUG and HOTKEYS are catalog-only command containers: a positive
catalog child invocation also exercises its container. No other parent receives
child credit; COMMAND has standalone behavior and requires its own invocation.
"""
from collections import deque
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CATALOG = json.loads((ROOT / "src/command_catalog.json").read_text())["commands"]
NAMES = {row["name"] for row in CATALOG}
PREFIXES = {tuple(name.split()) for name in NAMES}
MAX_WORDS = max(map(len, PREFIXES))
# These three official entries have no valid standalone invocation.
CONTAINERS = frozenset(("HIMPORT", "JSON.DEBUG", "HOTKEYS"))


def canonical(arguments):
    words = []
    for argument in arguments[:MAX_WORDS]:
        try:
            words.append(argument.decode("ascii").upper())
        except UnicodeDecodeError:
            break
    for length in range(len(words), 0, -1):
        if tuple(words[:length]) in PREFIXES:
            name = " ".join(words[:length])
            # These roots have no valid bare behavior or unlisted child.
            return None if name in CONTAINERS else name
    return None


def has_error(value):
    if isinstance(value, dict):
        return value.get("type") == "error" or any(has_error(item) for item in value.values())
    return isinstance(value, list) and any(has_error(item) for item in value)


def text_reply(value):
    """Text assertions deliberately validate, rather than erase, verbatim tags."""
    if isinstance(value, dict) and value.get("type") == "verbatim":
        assert set(value) == {"type", "format", "value"} and value["format"] == "txt", value
        value = value["value"]
    assert isinstance(value, str), value
    return value


class Evidence:
    def __init__(self):
        self.records = {}
        self.current = set()
        self.scenario = None

    def begin(self, scenario):
        self.current = set()
        self.scenario = scenario

    def positive(self, name):
        if name in NAMES:
            self.current.add(name)
            root, separator, _ = name.partition(" ")
            if separator and root in CONTAINERS:
                self.current.add(root)

    def finish(self, passed):
        if passed:
            for name in sorted(self.current):
                self.records.setdefault(name, {"command": name, "status": "positive", "scenario": self.scenario})
        self.current.clear()

    def save(self, path):
        Path(path).write_text(json.dumps(list(self.records.values()), indent=2) + "\n")


class WireEvidence:
    """Match actual CLI replies, including subscription and debugger events."""
    def __init__(self, evidence):
        self.evidence = evidence
        self.pending = deque()
        self.channels = {kind: set() for kind in ("subscribe", "psubscribe", "ssubscribe")}
        self.enabled, self.skip = True, False
        self.debug_enabled = self.debug_session = self.debug_paused = False

    def sent(self, arguments):
        name = canonical(arguments)
        # Only identity and reply-routing state survive the write, not argv.
        first = arguments[0].upper()
        if self.debug_paused:
            if first[:1] in (b"S", b"N", b"C", b"A"):
                self.debug_paused = False
            return
        expects = self.enabled
        if first == b"RESET" and len(arguments) == 1:
            self.enabled, self.skip, expects = True, False, True
        elif (len(arguments) == 3 and first == b"CLIENT" and arguments[1].upper() == b"REPLY"
              and arguments[2].upper() in (b"ON", b"OFF", b"SKIP")):
            mode = arguments[2].upper()
            if mode == b"ON":
                self.enabled, self.skip, expects = True, False, True
            elif mode == b"OFF":
                self.enabled, self.skip, expects = False, False, False
            elif mode == b"SKIP":
                self.skip, expects = True, False
        elif self.skip:
            self.skip, expects = False, False
        debug_mode = None
        if len(arguments) == 3 and first == b"SCRIPT" and arguments[1].upper() == b"DEBUG":
            mode = arguments[2].upper()
            if mode in (b"YES", b"SYNC", b"NO"):
                debug_mode = mode != b"NO"
        if self.debug_enabled and first in (b"EVAL", b"EVALSHA"):
            self.debug_session = True
        if not expects:
            return
        kind = first.decode("ascii", errors="replace").lower()
        subscription = kind in self.channels or kind in ("unsubscribe", "punsubscribe", "sunsubscribe")
        subscribe_kind = kind.replace("unsubscribe", "subscribe")
        remaining = max(1, len(arguments) - 1) if len(arguments) > 1 else max(1, len(self.channels.get(subscribe_kind, ())))
        self.pending.append({"name": name, "kind": kind if subscription else None,
                             "remaining": remaining, "debug": debug_mode, "reset": first == b"RESET"})
        assert len(self.pending) <= 4096, "BDD pending wire evidence bound exceeded"

    def received(self, reply):
        while isinstance(reply, dict) and reply.get("type") == "attribute":
            assert set(reply) == {"type", "entries", "value"}, reply
            reply = reply["value"]
        push = isinstance(reply, dict) and reply.get("type") == "push"
        values = reply.get("values") if push else reply
        if self.debug_session:
            if isinstance(values, list):
                if values == ["<endsession>"]:
                    self.debug_session = self.debug_paused = False
                elif values and isinstance(values[0], str) and values[0].startswith("* Stopped at "):
                    self.debug_paused = True
                return
            if has_error(reply) and self.debug_paused:
                return
        if not self.pending:
            return
        pending = self.pending[0]
        if (push or pending["kind"] is not None) and isinstance(values, list) and len(values) == 3 and values[0] in (
                "subscribe", "psubscribe", "ssubscribe", "unsubscribe", "punsubscribe", "sunsubscribe"):
            if pending["kind"] != values[0]:
                return
            assert type(values[2]) is int and values[2] >= 0, reply
            subscribe_kind = values[0].replace("unsubscribe", "subscribe")
            # Canonical JSON scalars/bytes tags make a bounded stable key.
            channel = json.dumps(values[1], sort_keys=True)
            if values[1] is not None:
                if "unsubscribe" in values[0]:
                    self.channels[subscribe_kind].discard(channel)
                else:
                    self.channels[subscribe_kind].add(channel)
            pending["remaining"] -= 1
            if pending["remaining"]:
                return
        elif push or (isinstance(values, list) and values and values[0] in ("message", "pmessage", "smessage")
                      and any(self.channels.values())):
            return
        self.pending.popleft()
        self.debug_session = self.debug_paused = False
        if has_error(reply):
            return
        if pending["debug"] is not None:
            self.debug_enabled = pending["debug"]
        if pending["reset"]:
            self.debug_enabled = False
            for channels in self.channels.values():
                channels.clear()
        if pending["name"] not in ("SYNC", "PSYNC"):
            self.evidence.positive(pending["name"])

    def shutdown_completed(self):
        assert self.pending and self.pending[0]["name"] == "SHUTDOWN", "No pending SHUTDOWN"
        self.evidence.positive(self.pending.popleft()["name"])
