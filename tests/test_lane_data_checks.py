"""deploy/run-update.sh runs the lane data checks before it commits (test-suite audit 2026-11, R-E7).

With RUN_TESTS=1 the lane runs the offline pytest suite after the validator, and flags a run that changed the
fixed ISP A3 reference (data/isp_rez_curtailment.feather). A failure turns the lane red; by default the run still
publishes (warn-only), and LANE_TESTS_BLOCK_PUSH=1 holds it back. A venv without pytest skips the suite with a
WARN. No network: a local bare origin and a stub PYTHON stand in for GitHub, the pipeline and pytest.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "deploy" / "run-update.sh"
REF = "data/isp_rez_curtailment.feather"

# $1 -c: the pytest probe; $2 names the module for -m calls; `tests/validate_outputs.py` passes.
STUB = """#!/usr/bin/env bash
if [[ "$1" == "-c" ]]; then exit "${STUB_IMPORT_RC:-0}"; fi
case "$2" in
  src.main)
    printf '%s' "${NEW_SUMMARY}" > outputs/summary.csv
    if [[ -n "${TOUCH_REF:-}" ]]; then printf 'rebuilt' > data/isp_rez_curtailment.feather; fi
    if [[ -n "${CHMOD_REF:-}" ]]; then chmod +x data/isp_rez_curtailment.feather; fi
    exit 0 ;;
  pytest) shift 2; echo "$*" >> "${STUB_LOG}"; exit "${STUB_PYTEST_RC:-0}" ;;
  src.post_publish_check) exit "${CHECK_RC:-0}" ;;
esac
"""

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")


def _git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout


@pytest.fixture
def lane(tmp_path):
    origin, app, log = tmp_path / "origin.git", tmp_path / "app", tmp_path / "pytest.log"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(app))
    _git(app, "checkout", "-q", "-b", "main")
    (app / "outputs").mkdir()
    (app / "data").mkdir()
    (app / "outputs" / "summary.csv").write_text("DUID\nA\n")
    (app / REF).write_text("committed A3 table")
    _git(app, "add", ".")
    _git(app, "commit", "-qm", "init")
    _git(app, "push", "-q", "origin", "main")
    stub = tmp_path / "python"
    stub.write_text(STUB)
    stub.chmod(0o755)

    def run(**extra):
        env = os.environ | {"APP_DIR": str(app), "PYTHON": str(stub), "RUN_TESTS": "1", "PUSH_CHANGES": "1",
                            "STUB_LOG": str(log), "NEW_SUMMARY": "DUID\nA\nB\n",
                            "GIT_AUTHOR_NAME": "bot", "GIT_AUTHOR_EMAIL": "bot@example.invalid",
                            "GIT_COMMITTER_NAME": "bot", "GIT_COMMITTER_EMAIL": "bot@example.invalid"}
        env.pop("LANE_TESTS_BLOCK_PUSH", None)
        env.update(extra)
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    def published():
        return _git(app, "--git-dir", str(origin), "show", "main:outputs/summary.csv")

    return {"app": app, "log": log, "run": run, "published": published}


def test_suite_runs_before_the_commit_and_a_pass_publishes(lane):
    run = lane["run"]()
    assert run.returncode == 0, run.stdout + run.stderr
    assert lane["log"].read_text().split() == ["-q", "-p", "no:cacheprovider", "--basetemp",
                                              str(lane["app"] / ".lane-pytest-tmp"), "tests"]
    assert lane["published"]() == "DUID\nA\nB\n"
    assert not (lane["app"] / ".lane-pytest-tmp").exists()


def test_failed_suite_is_red_but_still_publishes_by_default(lane):
    run = lane["run"](STUB_PYTEST_RC="1")
    assert run.returncode == 1, run.stdout + run.stderr
    assert "WARN: the offline test suite failed" in run.stdout
    assert "Lane data checks failed: pytest tests (exit 1)" in run.stdout
    assert lane["published"]() == "DUID\nA\nB\n"


def test_block_push_switch_holds_back_a_failed_run(lane):
    run = lane["run"](STUB_PYTEST_RC="1", LANE_TESTS_BLOCK_PUSH="1")
    assert run.returncode == 1, run.stdout + run.stderr
    assert "held this run back" in run.stdout
    assert lane["published"]() == "DUID\nA\n"
    assert _git(lane["app"], "status", "--porcelain") == ""


def test_block_push_switch_publishes_a_passing_run(lane):
    run = lane["run"](LANE_TESTS_BLOCK_PUSH="1")
    assert run.returncode == 0, run.stdout + run.stderr
    assert lane["published"]() == "DUID\nA\nB\n"


def test_venv_without_pytest_warns_and_publishes_as_before(lane):
    run = lane["run"](STUB_IMPORT_RC="1", LANE_TESTS_BLOCK_PUSH="1")
    assert run.returncode == 0, run.stdout + run.stderr
    assert "WARN: pytest is not importable" in run.stdout
    assert not lane["log"].exists()
    assert lane["published"]() == "DUID\nA\nB\n"


def test_a_changed_isp_a3_reference_is_flagged(lane):
    run = lane["run"](TOUCH_REF="1")
    assert run.returncode == 1, run.stdout + run.stderr
    assert f"WARN: {REF} differs from HEAD" in run.stdout
    assert f"Lane data checks failed: {REF} changed" in run.stdout


def test_a_changed_isp_a3_reference_is_flagged_even_with_nothing_to_publish(lane):
    run = lane["run"](TOUCH_REF="1", NEW_SUMMARY="DUID\nA\n")
    assert run.returncode == 1, run.stdout + run.stderr
    assert "No canonical summary.csv changes" in run.stdout
    assert f"{REF} changed" in run.stdout


def test_a_mode_only_change_to_the_reference_is_not_flagged(lane):
    """b2c46c0 changed only the file mode (same blob): content is what must not move."""
    run = lane["run"](CHMOD_REF="1")
    assert run.returncode == 0, run.stdout + run.stderr
    assert "differs from HEAD" not in run.stdout


def test_red_post_publish_check_keeps_its_exit_code(lane):
    run = lane["run"](STUB_PYTEST_RC="1", CHECK_RC="3")
    assert run.returncode == 3, run.stdout + run.stderr
