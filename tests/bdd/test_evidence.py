"""Deterministic reply-routing regressions; run separately from server BDD."""
import json
import tempfile
import unittest
from pathlib import Path

from evidence import Evidence, WireEvidence, canonical, text_reply


class WireEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.evidence = Evidence()
        self.evidence.begin("example.feature:3")
        self.wire = WireEvidence(self.evidence)

    def send(self, *arguments):
        self.wire.sent([argument.encode() for argument in arguments])

    def test_catalog_identity_is_longest_and_never_arguments(self):
        self.assertEqual(canonical([b"cluster", b"slot-stats", b"SLOTSRANGE", b"0", b"1"]), "CLUSTER SLOT-STATS")
        self.assertEqual(canonical([b"GET", b"secret"]), "GET")
        self.assertIsNone(canonical([b"not-a-command", b"GET"]))
        self.assertIsNone(canonical([b"\xff"]))

    def test_only_explicit_catalog_containers_receive_positive_child_credit(self):
        for arguments in (
                ("HIMPORT", "PREPARE", "layout", "field"),
                ("JSON.DEBUG", "MEMORY", "doc"),
                ("HOTKEYS", "START")):
            self.send(*arguments)
            self.wire.received("OK")
        self.assertEqual(self.evidence.current, {
            "HIMPORT", "HIMPORT PREPARE", "JSON.DEBUG", "JSON.DEBUG MEMORY",
            "HOTKEYS", "HOTKEYS START"})
        self.send("COMMAND", "INFO", "GET")
        self.wire.received([["get"]])
        self.assertIn("COMMAND INFO", self.evidence.current)
        self.assertNotIn("COMMAND", self.evidence.current)
        self.send("COMMAND")
        self.wire.received([["get"]])
        self.assertIn("COMMAND", self.evidence.current)

    def test_container_errors_and_unknown_children_do_not_earn_child_credit(self):
        self.send("HOTKEYS", "START")
        self.wire.received({"type": "error", "value": "ERR already tracking"})
        self.send("HIMPORT", "unknown-child")
        self.wire.received("OK")
        self.assertFalse(self.evidence.current)
        for root in ("HIMPORT", "JSON.DEBUG", "HOTKEYS"):
            self.assertIsNone(canonical([root.encode()]))
            self.assertIsNone(canonical([root.encode(), b"unknown-child"]))

    def test_only_matched_nonerror_replies_in_passed_scenarios_count(self):
        self.wire.received("unsolicited")
        self.send("SET", "key", "secret")
        self.wire.received({"type": "error", "value": "NOPERM secret"})
        self.send("not-a-command")
        self.wire.received("OK")
        self.assertFalse(self.evidence.current)
        self.send("GET", "key")
        self.wire.received(None)
        self.evidence.finish(False)
        self.assertFalse(self.evidence.records)
        self.evidence.begin("example.feature:9")
        self.send("SET", "key", "secret")
        self.wire.received("OK")
        self.evidence.finish(True)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "evidence.json"
            self.evidence.save(path)
            records = json.loads(path.read_text())
        self.assertEqual(records, [{"command": "SET", "status": "positive", "scenario": "example.feature:9"}])

    def test_nested_errors_do_not_count(self):
        self.send("EXEC")
        self.wire.received(["OK", {"type": "error", "value": "ERR failure"}])
        self.assertFalse(self.evidence.current)

    def test_reply_suppression_and_reset_keep_pending_alignment(self):
        self.send("CLIENT", "REPLY", "OFF")
        self.send("SET", "secret", "value")
        self.send("CLIENT", "REPLY", "SKIP")
        self.send("PING")
        self.send("CLIENT", "REPLY", "ON")
        self.wire.received("OK")
        self.send("CLIENT", "REPLY", "SKIP")
        self.send("CLIENT", "REPLY", "invalid")
        self.send("ECHO", "visible")
        self.wire.received("visible")
        self.send("CLIENT", "REPLY", "OFF")
        self.send("RESET")
        self.wire.received("RESET")
        self.send("GET", "secret")
        self.wire.received("value")
        self.assertEqual(self.evidence.current, {"CLIENT REPLY", "ECHO", "RESET", "GET"})
        self.assertFalse(self.wire.pending)

    def test_debugger_events_and_commands_preserve_pending_eval(self):
        self.send("SCRIPT", "DEBUG", "SYNC")
        self.wire.received("OK")
        self.send("EVAL", "local n = 1\nreturn n + 1", "0")
        self.wire.received(["* Stopped at 1", "-> 1 local n = 1"])
        self.send("list")
        self.wire.received(["-> 1 local n = 1", "   2 return n + 1"])
        self.send("invalid-debugger-command")
        self.wire.received({"type": "error", "value": "ERR Unknown Lua debugger command"})
        self.assertEqual(len(self.wire.pending), 1)
        self.assertNotIn("EVAL", self.evidence.current)
        self.send("step")
        self.wire.received(["* Stopped at 2", "-> 2 return n + 1"])
        self.send("continue")
        self.wire.received(["<endsession>"])
        self.assertEqual(len(self.wire.pending), 1)
        self.wire.received(2)
        self.send("PING")
        self.wire.received("PONG")
        self.assertEqual(self.evidence.current, {"SCRIPT DEBUG", "EVAL", "PING"})
        self.assertFalse(self.wire.pending)

    def test_subscription_pushes_wait_for_all_acknowledgements(self):
        self.send("SUBSCRIBE", "a", "b")
        self.wire.received({"type": "push", "values": ["subscribe", "a", 1]})
        self.wire.received({"type": "push", "values": ["message", "a", "payload"]})
        self.assertNotIn("SUBSCRIBE", self.evidence.current)
        self.wire.received({"type": "push", "values": ["subscribe", "b", 2]})
        self.send("GET", "key")
        self.wire.received({"type": "push", "values": ["message", "a", "payload"]})
        self.assertNotIn("GET", self.evidence.current)
        self.wire.received("value")
        self.send("UNSUBSCRIBE")
        self.wire.received({"type": "push", "values": ["unsubscribe", "a", 1]})
        self.wire.received({"type": "push", "values": ["unsubscribe", "b", 0]})
        self.assertEqual(self.evidence.current, {"SUBSCRIBE", "UNSUBSCRIBE", "GET"})

    def test_ordinary_ack_shaped_arrays_and_attributes_are_replies(self):
        self.send("EVAL", "return {'subscribe','value',1}", "0")
        self.wire.received({"type": "attribute", "entries": [], "value": ["subscribe", "value", 1]})
        self.assertIn("EVAL", self.evidence.current)

    def test_replication_headers_cannot_claim_protocol_hijack_success(self):
        self.send("SYNC")
        self.wire.received("FULLRESYNC not-a-stream")
        self.send("PSYNC", "?", "-1")
        self.wire.received("CONTINUE")
        self.assertFalse(self.evidence.current)

    def test_verbatim_text_validates_tags_and_preserves_binary_distinction(self):
        self.assertEqual(text_reply({"type": "verbatim", "format": "txt", "value": "cluster_state:ok"}), "cluster_state:ok")
        for value in ({"type": "bytes", "base64": "AP8="},
                      {"type": "verbatim", "format": "mkd", "value": "text"}):
            with self.assertRaises(AssertionError):
                text_reply(value)


if __name__ == "__main__":
    unittest.main()
