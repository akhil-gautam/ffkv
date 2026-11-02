"""Private native-history regressions using only scenario-owned processes."""
from behave import when, then
from native_history_runtime import (
    queue_source_time, chained_elapsed_payload, chained_history_replacement,
    assert_queue_source_time, assert_chained_elapsed_payload, assert_chained_history_replacement,
    chained_promotion_continue, assert_chained_promotion_continue,
)


@when("an owned native follower crosses a queue deadline while its source is stopped")
def queue_deadline(context):
    context.native_history_result = queue_source_time(context.runtime)


@then("the queued source transition applies once without replacement synchronization")
def queue_verified(context):
    assert_queue_source_time(context.native_history_result)


@when("an owned chained follower snapshots an elapsed list before its source PERSIST arrives")
def elapsed_snapshot(context):
    context.native_history_result = chained_elapsed_payload(context.runtime)


@then("every chained native consumer retains the persistent list payload")
def elapsed_verified(context):
    assert_chained_elapsed_payload(context.native_history_result)


@when("an owned middle replica installs unrelated history at its downstream offset")
def replace_history(context):
    context.native_history_result = chained_history_replacement(context.runtime)


@then("the chained consumer installs the replacement image before receiving its tail")
def history_verified(context):
    assert_chained_history_replacement(context.native_history_result)


@when("an owned middle replica is promoted while its chained follower is disconnected")
def promote_middle(context):
    context.native_history_result = chained_promotion_continue(context.runtime)


@then("the chained follower partially resumes the promoted history with its data intact")
def promotion_verified(context):
    assert_chained_promotion_continue(context.native_history_result)
