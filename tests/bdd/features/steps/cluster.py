"""Cluster-only fixture steps; all state is observed through owned native clients."""
import json

from behave import given, when, then

from cluster_runtime import ClusterFixture, ClusterWriter, IDENTIFIER, fields, key_for_slot, key_slot, node_rows
from runtime import assert_equal
from evidence import text_reply


def cluster(context):
    assert hasattr(context, "cluster"), "start an owned cluster fixture first"
    return context.cluster


def expand(context, value):
    runtime = context.runtime
    if runtime.default_node is not None:
        runtime = runtime.node(runtime.default_node)
    return runtime.argument(value).decode()


def document(context):
    assert context.text is not None, "JSON docstring required"
    text = context.text
    for name, value in context.runtime.placeholders.items():
        text = text.replace("{{" + name + "}}", str(value))
    return json.loads(text)


@given("a fresh ffkv cluster with 3 primaries and 3 replicas")
def fresh_cluster(context):
    context.cluster = ClusterFixture(context.runtime, 6, assigned=True)


@given("a fresh unassigned ffkv cluster with 2 nodes")
def fresh_unassigned_cluster(context):
    context.cluster = ClusterFixture(context.runtime, 2, assigned=False)


@given("a fresh isolated cluster-enabled ffkv node")
def isolated_cluster(context):
    context.cluster = ClusterFixture(context.runtime, 1, assigned=False)


@given("a fresh ffkv cluster with slot statistics enabled")
def metric_cluster(context):
    context.cluster = ClusterFixture(context.runtime, 3, assigned=True, metrics=True)


@given('cluster node "{name}" uses RESP {version:d}')
def node_version(context, name, version):
    node = cluster(context).node(name)
    if "default" in node.clients:
        node.disconnect("default")
    node.set_version("default", version)


@given('the cluster configuration directory of "{name}" is unwritable')
def unwritable_config(context, name):
    cluster(context).fault_configuration_directory(name)


@when('cluster node "{name}" restarts after a crash')
def restart(context, name):
    cluster(context).restart(name)


@when('cluster node "{name}" crashes')
def crash(context, name):
    cluster(context).crash(name)


@when('cluster node "{name}" is suspended')
def suspend(context, name):
    cluster(context).pause(name)


@when('cluster node "{name}" resumes')
def resume(context, name):
    cluster(context).resume(name)


@then('cluster node "{name}" eventually returns:')
def eventually_returns(context, name):
    spec = document(context)
    assert set(spec) == {"command", "reply"}, spec
    cluster(context).wait_reply(name, spec["command"], spec["reply"])


@then('the cluster eventually agrees on slot {slot} owned by "{owner}"')
def slot_owner(context, slot, owner):
    cluster(context).wait_owner(int(expand(context, slot)), owner)


@then('the cluster eventually agrees on slot {slot} owned by "{owner}" among running nodes')
def running_slot_owner(context, slot, owner):
    cluster(context).wait_owner(int(expand(context, slot)), owner)


@then('cluster node "{name}" has {count:d} known nodes')
def known_nodes(context, name, count):
    fixture = cluster(context)
    fixture.eventually((name, "known node count", count),
                       lambda: node_rows(fixture.command(name, ["CLUSTER", "NODES"])),
                       lambda rows: len(rows) == count)


@then("the reply is a 40 character hexadecimal identifier")
def identifier(context):
    reply = context.runtime.last()
    assert isinstance(reply, str) and IDENTIFIER.fullmatch(reply), reply


@then('the reply contains text "{text}"')
def reply_text(context, text):
    reply = text_reply(context.runtime.last())
    expected = expand(context, text)
    assert expected in reply, (reply, expected)


@given('the cluster bus pauses "{name}" snapshot chunks')
def arm_snapshot(context, name):
    cluster(context).proxies[name].arm()


@then('the cluster bus snapshot from "{name}" is paused')
def snapshot_paused(context, name):
    cluster(context).proxies[name].wait_paused()


@when('the cluster bus releases "{name}" snapshot chunks')
def release_snapshot(context, name):
    cluster(context).proxies[name].release("snapshot")


@given('the cluster bus pauses "{name}" mutation chunks')
def arm_mutations(context, name):
    cluster(context).proxies[name].arm("mutation")


@then('the cluster bus mutation stream from "{name}" is paused')
def mutation_stream_paused(context, name):
    cluster(context).proxies[name].wait_paused("mutation")


@when('the cluster bus releases "{name}" mutation chunks')
def release_mutations(context, name):
    cluster(context).proxies[name].release("mutation")


@given('cluster node "{name}" contains {count:d} keys in slot {slot:d} with {size:d} byte values')
def populate_slot(context, name, count, slot, size):
    assert 0 <= count <= 4096 and 0 <= size <= 8 * 1024 * 1024, (count, size)
    fixture = cluster(context)
    key = key_for_slot(slot, "payload")
    assert key_slot(key) == slot, (key, slot)
    value = {"repeat": {"text": "x", "count": size}}
    for index in range(count):
        fixture.command(name, ["SET", f"{key}:{index}", value])
    fixture.wait_reply(name, ["CLUSTER", "COUNTKEYSINSLOT", slot], count)


@given('cluster writer "{name}" on "{node}" writes key "{key}"')
def writer_ready(context, name, node, key):
    fixture = cluster(context)
    assert name not in fixture.writers, (name, "writer already exists")
    fixture.writers[name] = ClusterWriter(fixture, name, node, expand(context, key))


@when('cluster writer "{name}" continues')
def writer_continue(context, name):
    writer = cluster(context).writers[name]
    writer.raise_failure()
    writer.sent.clear()
    writer.release.set()
    assert writer.sent.wait(10), (name, "writer did not send after the start barrier")
    writer.raise_failure()


@then('cluster writer "{name}" acknowledges {count:d} more writes')
def writer_acknowledges(context, name, count):
    assert count > 0, count
    cluster(context).writers[name].advance(count)


@when('cluster writer "{name}" stops')
def writer_stop(context, name):
    cluster(context).writers[name].close()


@then('cluster writer "{name}" last value is on "{node}"')
def writer_value(context, name, node):
    fixture = cluster(context)
    writer = fixture.writers[name]
    assert not writer.thread.is_alive(), (name, "stop writer before asserting its final acknowledged value")
    writer.raise_failure()
    fixture.wait_reply(node, ["GET", writer.key], writer.value)


@then('cluster node "{name}" eventually has role "{role}"')
def cluster_role(context, name, role):
    fixture = cluster(context)
    aliases = {"primary": "master", "replica": "slave"}
    wanted = aliases.get(role, role)
    fixture.eventually((name, "replication role", role),
                       lambda: fields(fixture.command(name, ["INFO", "replication"])),
                       lambda reply: reply.get("role") == wanted)


@then('cluster node "{name}" eventually reports slot {slot} statistics:')
def slot_statistics(context, name, slot):
    fixture = cluster(context)
    slot = int(expand(context, slot))
    expected = document(context)
    fixture.wait_reply(name, ["CLUSTER", "SLOT-STATS", "SLOTSRANGE", slot, slot],
                       [[slot, expected]], subset=True, observer=True)


@then('cluster node "{name}" has a migration in state "{state}"')
def migration_state(context, name, state):
    fixture = cluster(context)
    fixture.eventually((name, "migration state", state),
                       lambda: fixture.command(name, ["CLUSTER", "MIGRATION", "STATUS", "ALL"]),
                       lambda reply: isinstance(reply, list) and any(task.get("state") == state for task in reply))


def mapping(reply):
    if isinstance(reply, dict):
        return reply
    assert isinstance(reply, list) and len(reply) % 2 == 0, ("expected RESP map", reply)
    names = reply[::2]
    assert all(isinstance(name, str) for name in names) and len(set(names)) == len(names), reply
    return dict(zip(names, reply[1::2]))


@then('the reply does not equal the slot of "{key}"')
def different_slot(context, key):
    reply = context.runtime.last()
    expected = key_slot(expand(context, key))
    assert type(reply) is int and reply != expected, (reply, expected)


@then('the reply equals saved "{name}"')
def saved_equal(context, name):
    assert_equal(context.runtime.last(), context.runtime.saved[name])


@then('the reply differs from saved "{name}"')
def saved_different(context, name):
    actual, previous = context.runtime.last(), context.runtime.saved[name]
    assert type(actual) is type(previous) and actual != previous, (actual, previous)


@then('the reply has a real cluster link to "{name}"')
def real_link(context, name):
    fixture = cluster(context)
    reply = context.runtime.last()
    assert isinstance(reply, list), reply
    links = [mapping(row) for row in reply]
    matching = [row for row in links if row.get("node") == fixture.ids[name]]
    assert matching, (name, links)
    for row in matching:
        assert row.get("direction") in ("to", "from") and row.get("create-time", 0) > 0, row
        assert "r" in row.get("events", ""), row
        assert row.get("send-buffer-used", -1) >= 0, row
    with fixture.proxies[name].lock:
        assert fixture.proxies[name].connections, (name, "no actual bus proxy connection exists")


@then("the reply describes exactly one myself node")
def exactly_one_myself(context):
    rows = node_rows(context.runtime.last())
    myself = [node_id for node_id, row in rows.items() if "myself" in row["flags"]]
    assert len(myself) == 1 and myself[0] in cluster(context).ids.values(), (myself, rows)


@then("the reply covers all 16384 slots exactly once with the fixture primaries")
def topology_slots(context):
    fixture = cluster(context)
    reply = context.runtime.last()
    assert isinstance(reply, list) and reply, reply
    cursor = 0
    owners = set()
    for row in sorted(reply, key=lambda value: value[0]):
        assert row[0] == cursor and row[0] <= row[1] < 16384, (cursor, row)
        owner = row[2]
        assert owner[2] in [fixture.ids[name] for name in fixture.names[:3]], row
        name = next(name for name in fixture.names[:3] if fixture.ids[name] == owner[2])
        assert owner[:2] == list(fixture.node(name).address), (name, owner)
        owners.add(owner[2])
        cursor = row[1] + 1
    assert cursor == 16384 and owners == {fixture.ids[name] for name in fixture.names[:3]}, (cursor, owners)


@then("the reply groups the fixture primaries with their actual replicas")
def topology_shards(context):
    fixture = cluster(context)
    reply = context.runtime.last()
    assert isinstance(reply, list) and len(reply) == 3, reply
    groups = [mapping(row) for row in reply]
    for primary, replica in zip(fixture.names[:3], fixture.names[3:]):
        matching = [group for group in groups if
                    {mapping(node)["id"] for node in group["nodes"]} == {fixture.ids[primary], fixture.ids[replica]}]
        assert len(matching) == 1, (primary, replica, groups)
        group = matching[0]
        nodes = {mapping(node)["id"]: mapping(node) for node in group["nodes"]}
        assert nodes[fixture.ids[primary]]["role"] == "master", group
        assert nodes[fixture.ids[replica]]["role"] in ("replica", "slave"), group
        for name in (primary, replica):
            assert nodes[fixture.ids[name]]["port"] == fixture.node(name).address[1], (name, group)
            assert nodes[fixture.ids[name]]["health"] == "online", (name, group)
        expected = [boundary for row in fixture.initial_slots if row[2][2] == fixture.ids[primary]
                    for boundary in row[:2]]
        assert_equal(group["slots"], expected)


@then('the reply lists cluster replica "{name}"')
def lists_replica(context, name):
    fixture = cluster(context)
    reply = context.runtime.last()
    assert isinstance(reply, list) and reply, reply
    rows = node_rows("\n".join(reply))
    assert fixture.ids[name] in rows, (name, rows)
    actual = rows[fixture.ids[name]]
    live = node_rows(fixture.command(name, ["CLUSTER", "NODES"]))[fixture.ids[name]]
    assert actual["parent"] == live["parent"] and actual["parent"] != "-", (name, actual, live)
    assert "slave" in actual["flags"] or "replica" in actual["flags"], actual


@then("the reply has 1 item selected from:")
def selected_key(context):
    reply, choices = context.runtime.last(), document(context)
    assert isinstance(reply, list) and len(reply) == 1 and reply[0] in choices, (reply, choices)


@then('cluster node "{name}" migration "{saved}" eventually has state "{state}"')
def saved_migration_state(context, name, saved, state):
    fixture = cluster(context)
    task_id = context.runtime.saved[saved]
    assert isinstance(task_id, str) and IDENTIFIER.fullmatch(task_id), (saved, task_id)
    fixture.eventually((name, "migration", task_id, "state", state),
                       lambda: fixture.command(name, ["CLUSTER", "MIGRATION", "STATUS", "ID", task_id]),
                       lambda reply: isinstance(reply, list) and len(reply) == 1 and
                       reply[0].get("id") == task_id and reply[0].get("state") == state)


@then('cluster node "{name}" eventually has a failure report for "{failed}"')
def failure_report(context, name, failed):
    fixture = cluster(context)
    fixture.eventually((name, "real gossip failure report", failed),
                       lambda: fixture.command(name, ["CLUSTER", "COUNT-FAILURE-REPORTS", fixture.ids[failed]]),
                       lambda reply: type(reply) is int and reply > 0)


@when("the lowest-ID cluster primary is suspended after an acknowledged write")
def suspend_first_voter(context):
    fixture = cluster(context)
    primary = min(fixture.names[:3], key=lambda name: fixture.ids[name])
    replica = fixture.names[fixture.names.index(primary) + 3]
    slot = next(row[0] for row in fixture.initial_slots if row[2][2] == fixture.ids[primary])
    key = key_for_slot(slot, "automatic-election")
    context.cluster_failure = (primary, replica, slot, key)
    assert fixture.command(primary, ["SET", key, "acknowledged-before-loss"]) == "OK"
    assert fixture.command(primary, ["WAIT", "1", "5000"]) == 1
    observer = next(name for name in fixture.names[:3] if name != primary)
    assert fixture.command(observer, ["CLUSTER", "COUNT-FAILURE-REPORTS", fixture.ids[primary]]) == 0
    fixture.pause(primary)


@then("surviving primaries elect its replica with the acknowledged data")
def automatic_election(context):
    fixture = cluster(context)
    primary, replica, slot, key = context.cluster_failure
    observer = next(name for name in fixture.names[:3] if name != primary)
    failure_report(context, observer, primary)
    fixture.wait_owner(slot, replica)
    fixture.wait_reply(replica, ["GET", key], "acknowledged-before-loss")


@when("that failed cluster primary resumes")
def resume_failed_primary(context):
    cluster(context).resume(context.cluster_failure[0])


@then("all cluster nodes retain the elected owner")
def retained_elected_owner(context):
    _, replica, slot, _ = context.cluster_failure
    cluster(context).wait_owner(slot, replica)


@then('cluster node "{name}" owns the former slots of "{old}"')
def former_slots(context, name, old):
    fixture = cluster(context)
    previous = [row[:2] for row in fixture.initial_slots if row[2][2] == fixture.ids[old]]
    assert previous, (old, "no recorded original ownership", fixture.initial_slots)
    fixture.eventually((name, "owns former slots", old),
                       lambda: fixture.command(name, ["CLUSTER", "SLOTS"]),
                       lambda reply: all(any(row[0] <= slot <= row[1] and row[2][2] == fixture.ids[name]
                                             for row in reply)
                                         for start, end in previous for slot in range(start, end + 1)))


@then('the reply has positive slot metric "{metric}" for slot {slot}')
def positive_metric(context, metric, slot):
    slot = int(expand(context, slot))
    reply = context.runtime.last()
    assert isinstance(reply, list), reply
    rows = [row for row in reply if isinstance(row, list) and len(row) == 2 and row[0] == slot]
    assert len(rows) == 1, (slot, reply)
    value = mapping(rows[0][1]).get(metric)
    assert type(value) is int and value > 0, (slot, metric, rows[0])


@then("the reply is a positive integer at most {maximum:d}")
def positive_bounded_integer(context, maximum):
    reply = context.runtime.last()
    assert type(reply) is int and 0 < reply <= maximum, (reply, "expected positive integer at most", maximum)
