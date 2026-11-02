"""Consumer assertions for authorization evidence returned by the native CLI."""
import json
from behave import then
from runtime import is_error


@then("the ACL log has client metadata without these secrets:")
def acl_metadata_without_secrets(context):
    entries = context.runtime.last()
    assert isinstance(entries, list) and entries, entries
    for entry in entries:
        assert isinstance(entry, dict), entry
        info = entry["client-info"]
        assert isinstance(info, str), info
        fields = dict(field.split("=", 1) for field in info.split() if "=" in field)
        assert int(fields["id"]) > 0, info
        assert fields["addr"].startswith("127.0.0.1:"), info
        assert fields["user"] == entry["username"], entry
    encoded = json.dumps(entries)
    for secret in json.loads(context.text):
        assert secret not in encoded, (secret, entries)


@then('the EXEC reply contains an error with code "{code}"')
def exec_error_code(context, code):
    replies = context.runtime.last()
    assert isinstance(replies, list), replies
    assert any(is_error(reply) and reply["value"].split()[0] == code for reply in replies), replies
