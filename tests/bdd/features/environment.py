"""Each scenario owns its server, data directory and client connections."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from runtime import Runtime
from evidence import Evidence


def before_all(context):
    context.wire_evidence = Evidence()


def before_scenario(context, scenario):
    identity = f"{Path(scenario.filename).name}:{scenario.line}"
    assert len(identity) <= 2048, "BDD scenario identity bound exceeded"
    context.wire_evidence.begin(identity)
    context.runtime = Runtime(context.config.userdata, context.wire_evidence)


def after_scenario(context, scenario):
    context.runtime.close()
    context.wire_evidence.finish(scenario.status.name == "passed")


def after_all(context):
    report = context.config.userdata.get("evidence_report")
    if report:
        context.wire_evidence.save(report)
