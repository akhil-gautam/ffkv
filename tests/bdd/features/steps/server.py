"""Consumer contracts for server administration replies."""
from behave import then
import json


@then("the reply describes an ACL dry-run denial")
def acl_dryrun_denial(context):
    value = context.runtime.last()
    assert isinstance(value, str) and value != "OK", value


@then("latency history contains a sample of at least {minimum:d} milliseconds")
def latency_history(context, minimum):
    samples = context.runtime.last()
    assert isinstance(samples, list), samples
    assert all(isinstance(sample, list) and len(sample) == 2 and type(sample[0]) is int and sample[0] > 0 and type(sample[1]) is int and sample[1] >= 0 for sample in samples), samples
    assert any(sample[1] >= minimum for sample in samples), samples


@then('latest latency for "{event}" is at least {minimum:d} milliseconds')
def latency_latest(context, event, minimum):
    samples = context.runtime.last()
    assert isinstance(samples, list), samples
    matching = [sample for sample in samples if isinstance(sample, list) and len(sample) == 4 and sample[0] == event]
    assert len(matching) == 1, samples
    _, timestamp, latest, maximum = matching[0]
    assert type(timestamp) is int and timestamp > 0, matching[0]
    assert type(latest) is int and type(maximum) is int and maximum >= latest >= minimum, matching[0]


@then("the slow log contains the executed command:")
def slowlog_command(context):
    expected = json.loads(context.text)
    entries = context.runtime.last()
    assert isinstance(entries, list), entries
    matching = [entry for entry in entries if isinstance(entry, list) and len(entry) >= 4 and entry[3] == expected]
    assert matching, (expected, entries)
    for identifier, timestamp, duration, *_ in matching:
        assert type(identifier) is int and identifier >= 0, matching
        assert type(timestamp) is int and timestamp > 0, matching
        assert type(duration) is int and duration >= 0, matching
