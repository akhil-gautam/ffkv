"""MCP-only steps; existing BDD cleanup owns the isolated fixture lifetime."""
from behave import given, when, then
from mcp_smoke import FEATURE_COVERAGE, Fixture


@given('an isolated real ffkv MCP fixture')
def isolated_mcp(context):
    userdata = context.config.userdata
    context.mcp_fixture = Fixture(userdata.get('server'), userdata.get('mcp'))
    context.runtime.cleanups.append(context.mcp_fixture.close)


@when('the MCP "{group}" behavior contract is exercised')
def exercise_mcp(context, group):
    assert group in FEATURE_COVERAGE, group
    context.mcp_fixture.run(group)


@then('the MCP "{group}" contract has passed with real replies')
def passed_mcp(context, group):
    assert group in context.mcp_fixture.completed, group
    assert FEATURE_COVERAGE[group], group
