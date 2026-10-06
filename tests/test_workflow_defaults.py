"""The manual GitHub workflow runs the pipeline the way the NAS lane does (--full-refresh)."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _input_default(workflow: str, name: str) -> str:
    block = re.search(rf"^\s+{name}:\s*\n((?:\s{{8,}}.*\n)+)", workflow, re.MULTILINE)
    assert block, f"workflow input {name} not found"
    default = re.search(r"default:\s*['\"]?(\w+)['\"]?", block.group(1))
    assert default, f"workflow input {name} has no default"
    return default.group(1)


def test_nas_lane_full_refreshes_by_default():
    script = (ROOT / "deploy" / "run-update.sh").read_text()
    assert 'PIPELINE_ARGS="${PIPELINE_ARGS:---full-refresh}"' in script


def test_manual_workflow_defaults_to_full_refresh_like_the_nas_lane():
    workflow = (ROOT / ".github" / "workflows" / "update.yml").read_text()
    assert _input_default(workflow, "full_refresh") == "true"
