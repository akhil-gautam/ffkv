"""Private steps for scenario-owned replication fixtures."""
import hashlib
import json
import secrets
import socket
from behave import given, when, then
from replication_runtime import (
    NativeProbe, ReplicationFixture, dataset, eventually, frame, info,
)
from runtime import assert_equal


@given("an owned HTTP snapshot-polling replication pair")
def polling_pair(context):
    runtime = context.runtime
    token = secrets.token_hex(32)
    policy = runtime.directory / "polling-token-policy.json"
    credential = runtime.directory / "polling-token"
    policy.write_text(json.dumps({"tokens": [{"name": "owned-polling", "role": "admin", "sha256": hashlib.sha256(token.encode()).hexdigest()}]}))
    credential.write_text(token + "\n")
    credential.chmod(0o600)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    runtime.add_node("polling-source", [
        "--management-bind", f"127.0.0.1:{port}", "--management-token-file", str(policy),
        "--management-audit-file", str(runtime.directory / "polling-audit.jsonl"), "--replication-export",
        "--databases", "64",
    ])
    runtime.add_node("polling-follower", [
        "--replica-source", f"http://127.0.0.1:{port}", "--replica-token-file", str(credential),
        "--replica-allow-loopback-http", "--replica-interval-ms", "100", "--replica-timeout-ms", "5000",
        "--databases", "64",
    ])
    runtime.default_node = "polling-follower"


def fixture(context):
    assert hasattr(context, "replication"), "owned replication pair required"
    return context.replication


@given("an owned native replication pair not yet following")
def pair_not_following(context):
    context.replication = ReplicationFixture(context.runtime)


@given("an owned synchronized native replication pair")
def pair_synchronized(context):
    context.replication = ReplicationFixture(context.runtime, synchronized=True)


@given("an owned synchronized native replication pair with a {size:d} byte backlog")
def pair_backlog(context, size):
    context.replication = ReplicationFixture(context.runtime, synchronized=True, backlog=size)


@given("an owned native replication pair with protected upstream networking")
def pair_protected(context):
    context.replication = ReplicationFixture(context.runtime, protected=True)


@given("an owned native replication pair with primary AOF disabled")
def pair_aof_disabled(context):
    context.replication = ReplicationFixture(context.runtime, aof_disabled=True)


@when("the owned replication link is disconnected")
def disconnect(context):
    fixture(context).disconnect()


@when("the owned replication link is reconnected")
def reconnect(context):
    fixture(context).reconnect()


@then("the next replica synchronization is partial")
def partial(context):
    fixture(context).next_sync("partial")


@then("the next replica synchronization is full")
def full(context):
    fixture(context).next_sync("full")


@then("the resumed replication bytes equal the missing primary history bytes")
def resumed_bytes(context):
    fixture(context).assert_resumed_bytes()


@when("owned writes exceed the primary replication backlog")
def exceed_backlog(context):
    owned = fixture(context)
    before = int(info(owned.master)["master_repl_offset"])
    for index in range(16):
        reply = owned.master.execute(["SET", "backlog-overflow-" + str(index), "x" * 256], "__backlog_writer")
        assert reply == "OK", reply
        # Keep the independent observer current while the actual replica remains
        # disconnected; otherwise the observer would also fall out of backlog.
        owned.history_probe.through_offset(int(info(owned.master)["master_repl_offset"]))
    assert int(info(owned.master)["master_repl_offset"]) - before > 1024


@then("the replica dataset equals the primary dataset")
def same_dataset(context):
    owned = fixture(context)
    expected = dataset(owned.master)
    eventually(lambda: dataset(owned.replica), lambda actual: actual == expected, "full synchronization did not install the complete primary dataset")


@given('native probe "{name}" requests PSYNC with an unknown history')
def unknown_history(context, name):
    owned = fixture(context)
    assert name not in owned.probes, name
    owned.probes[name] = NativeProbe(owned, name, unknown=True)


@given('native probe "{name}" sends SYNC to node "{node}"')
def sync_probe(context, name, node):
    owned = fixture(context)
    assert name not in owned.probes, name
    owned.probes[name] = NativeProbe(owned, name, node=node, legacy=True)


@then('native probe "{name}" receives a FULLRESYNC header and a valid Redis RDB')
def full_probe(context, name):
    probe = fixture(context).probes[name]
    assert probe.header is not None and probe.header.startswith(b"+FULLRESYNC "), probe.header
    assert probe.apply_node is not None, "captured RDB must be installed by a real native follower"
    assert info(probe.apply_node)["master_link_status"] == "up"


@then('native probe "{name}" receives a valid Redis RDB without a FULLRESYNC header')
@then('native probe "{name}" receives a fresh valid Redis RDB without a FULLRESYNC header')
def legacy_probe(context, name):
    probe = fixture(context).probes[name]
    assert probe.legacy and probe.header is None
    assert probe.apply_node is not None, "captured RDB must be installed by a real native follower"
    assert info(probe.apply_node)["master_link_status"] == "up"


@then('native probe "{name}" applies the stream through key "{key}" with value "{value}"')
def apply_probe(context, name, key, value):
    probe = fixture(context).probes[name]
    probe.through_value(key, value)
    assert_equal(probe.apply_node.execute(["GET", key], "__probe_result"), value)
    eventually(lambda: list(probe.replay.acknowledgements), lambda values: any(int(args[2]) >= probe.offset and len(args) == 5 and int(args[4]) >= probe.write_offset for args in values), "real probe follower did not ACK its processed stream and FACK its applied writes")


@when('native probe "{name}" reconnects using SYNC')
def reconnect_probe(context, name):
    owned = fixture(context)
    old = owned.probes[name]
    old.close()
    owned.probes[name] = NativeProbe(owned, name, node=old.node_name, legacy=True)


@when("the owned replica fsync gate is closed")
def close_fsync(context):
    fixture(context).close_gate()


@when("the owned replica fsync gate is opened")
def open_fsync(context):
    owned = fixture(context)
    owned.assert_gate_entered()
    owned.open_gate()


@when("the owned replica restarts after a crash")
def crash_replica(context):
    fixture(context).crash_replica()


def ack_count(proxy):
    count = 0
    for request, _, _ in proxy.captures():
        position = 0
        while position < len(request):
            parsed = frame(request, position)
            if parsed is None:
                break
            args, position = parsed
            count += args[:2] == [b"REPLCONF", b"ACK"]
    return count


@when("the owned replication stream is paused without disconnecting ACK traffic")
def pause_stream(context):
    owned = fixture(context)
    owned.proxy.change(paused=True)
    baseline = ack_count(owned.proxy)
    eventually(lambda: ack_count(owned.proxy), lambda count: count > baseline, "stream gate also blocked replica-to-primary ACK traffic")


@when("the owned replication stream is resumed")
def resume_stream(context):
    fixture(context).proxy.change(paused=False)


@then('node "{node}" eventually has replication role "{role}"')
def role(context, node, role):
    wanted = "slave" if role == "replica" else role
    eventually(lambda: context.runtime.node(node).execute(["ROLE"], "__role_observer"), lambda result: isinstance(result, list) and result and result[0] == wanted, "native role transition " + node + " -> " + role)


@then('node "{node}" eventually follows node "{upstream}"')
def follows(context, node, upstream):
    target = context.runtime.node(upstream).address
    eventually(lambda: info(context.runtime.node(node)), lambda value: value.get("role") == "slave" and value.get("master_host") == target[0] and int(value.get("master_port", "0")) == target[1] and value.get("master_link_status") == "up", "native followership transition " + node + " -> " + upstream)


@then('node "{node}" eventually accepts writes after failover timeout')
def failover_timeout(context, node):
    runtime = context.runtime.node(node)
    eventually(lambda: runtime.execute(["SET", "__failover_timeout_barrier", "writable"], "__failover_observer"), lambda reply: reply == "OK", "failover timeout did not release client write admission")
    assert runtime.execute(["ROLE"], "__failover_observer")[0] == "master"


@when("the replica is configured with credentials for a protected-network upstream")
def protected_configure(context):
    fixture(context).protected_configure()


@then("no connection or credential bytes reach the protected-network upstream")
def protected_refusal(context):
    fixture(context).assert_protected()


@then('node "{node}" has no connected native upstream')
def no_upstream(context, node):
    status = info(context.runtime.node(node))
    assert status.get("role") == "slave" and status.get("master_link_status") == "down", status


@then("its preexisting dataset is unchanged")
def unchanged(context):
    owned = fixture(context)
    assert owned.before_dataset is not None
    assert_equal(dataset(owned.replica), owned.before_dataset)
