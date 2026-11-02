"""Shared wire-level Gherkin steps; every command uses the native CLI."""
import json
import math
import re
import select
import subprocess
import time
from pathlib import Path
from behave import given, when, then, register_type
from parse import with_pattern
from runtime import assert_equal, is_error
from evidence import text_reply


@with_pattern(r'[^"]+')
def quoted_name(value):
    return value


register_type(Quoted=quoted_name)


def document(context):
    assert context.text is not None, "JSON docstring required"
    value = json.loads(context.text)
    def expand(value):
        if isinstance(value, str):
            runtime = context.runtime
            target = runtime.nodes.get(runtime.default_node, runtime)
            values = dict(runtime.placeholders)
            if target.address is not None:
                values.update(host=target.address[0], port=str(target.address[1]))
            values["directory"] = str(target.directory)
            if "{{module_fixture}}" in value:
                values["module_fixture"] = str(target.module_fixture().resolve())
            for key, replacement in values.items():
                value = value.replace("{{" + key + "}}", str(replacement))
            return value
        if isinstance(value, list):
            return [expand(item) for item in value]
        if isinstance(value, dict):
            return {key: expand(item) for key, item in value.items()}
        return value
    return expand(value)


@given("a fresh ffkv server")
def fresh(context):
    context.runtime.start()


@given("the following commands succeed:")
@when("the following commands succeed:")
@then("the following commands succeed:")
def commands_succeed(context):
    for argv in document(context):
        reply = context.runtime.execute(argv)
        assert not is_error(reply), (argv, reply)


@when("I execute:")
def execute(context):
    context.runtime.execute(document(context))


@when('client "{name:Quoted}" executes:')
def named_execute(context, name):
    context.runtime.execute(document(context), name)


@when('client "{name:Quoted}" sends:')
def named_send(context, name):
    context.runtime.send(document(context), name)


@then("the reply equals:")
def reply_equals(context):
    assert_equal(context.runtime.last(), document(context))


@then('client "{name:Quoted}" receives:')
def receives(context, name):
    assert_equal(context.runtime.receive(name), document(context))


@then("the reply is an error")
def error_reply(context):
    assert is_error(context.runtime.last()), context.runtime.last()


@then('the reply is an error with code "{code}"')
def error_code(context, code):
    value = context.runtime.last()
    assert is_error(value), value
    assert value["value"].split()[0] == code, value


@then("the reply is null")
def null(context):
    assert context.runtime.last() is None, context.runtime.last()


@then("the reply contains:")
def contains(context):
    assert_equal(context.runtime.last(), document(context), subset=True)


@given('client "{name:Quoted}" uses RESP {version:d}')
def version(context, name, version):
    context.runtime.set_version(name, version)


@when('I save the reply as "{name}"')
def save(context, name):
    context.runtime.saved[name] = context.runtime.last()


@when("ffkv restarts after a crash")
def restart(context):
    context.runtime.restart_after_crash()


@then('client "{name:Quoted}" is blocked')
def blocked(context, name):
    context.runtime.wait_blocked(name)


@when('client "{name:Quoted}" disconnects')
def disconnect(context, name):
    context.runtime.disconnect(name)


@then("the reply has {count:d} items")
@then("the reply has length {count:d}")
def reply_length(context, count):
    value = context.runtime.last()
    if isinstance(value, dict) and value.get("type") == "set":
        value = value["values"]
    assert isinstance(value, (list, dict, str)), value
    assert len(value) == count, (len(value), count)


@then('the reply has the saved length "{name}"')
def saved_length(context, name):
    count = context.runtime.saved[name]
    assert type(count) is int and count >= 0, (name, count)
    reply_length(context, count)


@given('another fresh ffkv server named "{name}"')
def another_server(context, name):
    context.runtime.add_node(name)


@given("a fresh ffkv server with ACL rules:")
def acl_server(context):
    path = context.runtime.directory / "users.acl"
    path.write_text(context.text)
    path.chmod(0o600)
    context.runtime.extra_args.extend(["--acl-file", str(path)])
    context.runtime.start()


@when('server "{node}" executes:')
@when('node "{node}" executes:')
def node_execute(context, node):
    context.runtime.execute_node(node, document(context))


@when('client "{client:Quoted}" on node "{node:Quoted}" executes:')
def node_client_execute(context, client, node):
    context.runtime.execute_node(node, document(context), client)


@when('client "{client:Quoted}" on node "{node:Quoted}" sends:')
def node_client_send(context, client, node):
    runtime = context.runtime.node(node)
    runtime.send(document(context), client)
    context.runtime.last_native = runtime.last_native


@then('node "{node}" eventually replies:')
def node_eventually(context, node):
    spec = document(context)
    wait_reply(context, spec, node)


@when("I wait until executing:")
def wait_execute(context):
    wait_reply(context, document(context))


def wait_reply(context, spec, node=None):
    deadline = time.monotonic() + 15
    while True:
        actual = (context.runtime.execute_node(node, spec["command"]) if node is not None
                  else context.runtime.execute(spec["command"]))
        try:
            if "equals" in spec or "reply" in spec:
                assert_equal(actual, spec.get("equals", spec.get("reply")))
            if "contains" in spec:
                assert_equal(actual, spec["contains"], subset=True)
            if "text_contains" in spec:
                assert spec["text_contains"] in text_reply(actual), actual
            return
        except AssertionError:
            assert time.monotonic() < deadline, (spec, actual)
            time.sleep(0.01)


@then('client "{name:Quoted}" receives an error with code "{code}"')
def receive_error(context, name, code):
    context.runtime.receive(name)
    error_code(context, code)


@then('client "{name:Quoted}" has no reply for {milliseconds:d} milliseconds')
def no_reply(context, name, milliseconds):
    assert 0 <= milliseconds <= 5000
    context.runtime.client(name).quiet(milliseconds / 1000)


@then('client "{name:Quoted}" is disconnected')
def disconnected(context, name):
    client = context.runtime.client(name)
    assert select.select([client.process.stdout], [], [], 5)[0], (name, "connection not closed")
    assert client.process.stdout.read(1) == b"", (name, "unexpected reply before disconnect")
    client.process.wait(timeout=5)


@then("the reply is a positive integer")
def positive(context):
    integer_greater(context, 0)


@then("the reply is an integer greater than {minimum:d}")
def integer_greater(context, minimum):
    value = context.runtime.last()
    assert type(value) is int and value > minimum, (value, minimum)


@then("the reply is between {minimum:g} and {maximum:g}")
def between(context, minimum, maximum):
    value = context.runtime.last()
    assert type(value) in (int, float) and minimum <= value <= maximum, (value, minimum, maximum)


@then('the reply equals the saved value "{name}"')
def saved_equals(context, name):
    assert_equal(context.runtime.last(), context.runtime.saved[name])


@then('the reply does not equal the saved value "{name}"')
def saved_not_equals(context, name):
    assert context.runtime.last() != context.runtime.saved[name]


@then('the reply text contains "{text}"')
def text_contains(context, text):
    value = text_reply(context.runtime.last())
    assert text in value, (text, value)


@then('the reply text does not contain "{text}"')
@then('the received text does not contain "{text}"')
def text_excludes(context, text):
    value = text_reply(context.runtime.last())
    assert text not in value, (text, value)


@then('client "{name:Quoted}" receives text containing "{text}"')
def receive_text(context, name, text):
    context.runtime.receive(name)
    text_contains(context, text)


@then("the reply has {count:d} client records")
def client_records(context, count):
    value = context.runtime.last()
    assert isinstance(value, str), value
    records = [line for line in value.splitlines() if line]
    assert len(records) == count and all(line.startswith("id=") for line in records), value




@then("the reply is a hexadecimal string of length {count:d}")
def hexadecimal(context, count):
    value = context.runtime.last()
    assert isinstance(value, str) and len(value) == count and re.fullmatch("[0-9a-f]+", value), value


@then("the reply is a Redis timestamp pair")
def timestamp(context):
    value = context.runtime.last()
    assert isinstance(value, list) and len(value) == 2, value
    assert all(isinstance(item, str) and item.isdecimal() for item in value), value
    assert int(value[0]) > 0 and 0 <= int(value[1]) < 1_000_000, value


def lookup_path(value, path):
    while path:
        if isinstance(value, dict) and path in value:
            return value[path]
        head, _, path = path.partition(".")
        value = value[int(head)] if isinstance(value, list) else value[head]
    return value


@then('the reply has JSON path "{path}" equal to:')
def path_equals(context, path):
    assert_equal(lookup_path(context.runtime.last(), path), document(context))


@then('the reply has JSON path "{path}" with an integer greater than {minimum:d}')
def path_integer_greater(context, path, minimum):
    value = lookup_path(context.runtime.last(), path)
    assert type(value) is int and value > minimum, (path, value, minimum)


@then('the reply has JSON path "{path}" with a nonnegative number')
def path_nonnegative(context, path):
    value = lookup_path(context.runtime.last(), path)
    assert type(value) in (int, float) and math.isfinite(value) and value >= 0, (path, value)


@when('I save the reply at JSON pointer "{pointer}" as "{name}"')
def save_pointer(context, pointer, name):
    value = context.runtime.last()
    assert not pointer or pointer.startswith("/"), pointer
    for component in pointer.split("/")[1:]:
        component = component.replace("~1", "/").replace("~0", "~")
        value = value[int(component)] if isinstance(value, list) else value[component]
    context.runtime.saved[name] = value


@then("the reply contains the consecutive items:")
def consecutive(context):
    actual, expected = context.runtime.last(), document(context)
    assert isinstance(actual, list) and isinstance(expected, list) and expected
    for start in range(len(actual) - len(expected) + 1):
        try:
            assert_equal(actual[start:start + len(expected)], expected)
            return
        except AssertionError:
            pass
    raise AssertionError((actual, expected))


@then('function "{name}" is running')
def function_running(context, name):
    runtime = context.runtime
    observer = "__function_observer"
    if observer not in runtime.clients:
        runtime.set_version(observer, 3)
    deadline = time.monotonic() + 5
    while True:
        value = runtime.client(observer).execute(["FUNCTION", "STATS"])
        if isinstance(value, dict) and isinstance(value.get("running_script"), dict) and value["running_script"].get("name") == name:
            return
        assert time.monotonic() < deadline, (name, value)
        time.sleep(0.001)


@when("ffkv shuts down with SAVE")
def shutdown_saved(context):
    runtime = context.runtime
    runtime.send(["SHUTDOWN", "SAVE"])
    assert runtime.process.wait(timeout=15) == 0
    runtime.last_native.evidence.shutdown_completed()
    runtime.close_clients()
    runtime.process.stderr.close()
    runtime.process = None


@when("ffkv starts again")
def start_again(context):
    context.runtime.start()




@then('the file "{filename}" exists')
def owned_file_exists(context, filename):
    runtime = context.runtime
    target = runtime.nodes.get(runtime.default_node, runtime)
    path = Path(target.argument(filename).decode()).resolve()
    assert path.is_relative_to(target.directory.resolve()), ("BDD file is outside the owned directory", path)
    deadline = time.monotonic() + 5
    while not path.is_file():
        assert time.monotonic() < deadline, ("Owned file did not appear", path)
        time.sleep(0.01)


@when("ffkv restarts from the sealed backup")
def restore_sealed(context):
    runtime = context.runtime
    files = runtime.execute(["BACKUP", "LIST"])
    assert isinstance(files, list) and files, files
    paths = [Path(filename).resolve() for filename in files]
    assert all(path.is_relative_to(runtime.directory.resolve()) for path in paths)
    source = paths[0].parent
    assert all(path.parent == source for path in paths), paths
    runtime.close_clients()
    runtime.process.send_signal(__import__("signal").SIGINT)
    assert runtime.process.wait(timeout=15) == 0
    runtime.process.stderr.close()
    runtime.process = None
    restored = runtime.directory / "restored-sealed"
    result = subprocess.run([str(runtime.server_binary), "restore", "--source", str(source), "--data-dir", str(restored)],
                            capture_output=True, timeout=30)
    assert result.returncode == 0, (result.returncode, result.stderr)
    runtime.data_directory = restored
    runtime.start()
