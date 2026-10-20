"""deploy/run-update.sh starts every run from the committed outputs and feather caches.

A run that fails after src.main (e.g. in the validator) leaves outputs/ and data/*.feather edited
and uncommitted. The next run must neither take that summary as its "before" baseline (which would
drop the same new data as "no change") nor read or publish the leftovers. No network: a local bare
origin and a stub PYTHON stand in for GitHub and the pipeline.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "deploy" / "run-update.sh"

COMMITTED = "DUID\nA\n"
NEW = "DUID\nA\nUWF1\n"

# Stands in for the pipeline: records the summary and feather it would read, writes $NEW_SUMMARY.
STUB = """#!/usr/bin/env bash
case "$2" in
  src.main)
    cp outputs/summary.csv "${STUB_LOG}/input"
    cp data/x.feather "${STUB_LOG}/feather"
    printf '%s' "${NEW_SUMMARY}" > outputs/summary.csv
    exit 0 ;;
  src.post_publish_check) exit 0 ;;
esac
"""

pytestmark = pytest.mark.skipif(shutil.which("git") is None, reason="needs git")


def _git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout


@pytest.fixture
def lane(tmp_path):
    origin, app, log = tmp_path / "origin.git", tmp_path / "app", tmp_path / "log"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(app))
    _git(app, "checkout", "-q", "-b", "main")
    (app / "outputs").mkdir()
    (app / "data").mkdir()
    (app / "outputs" / "summary.csv").write_text(COMMITTED)
    (app / "data" / "x.feather").write_text("committed")
    _git(app, "add", ".")
    _git(app, "commit", "-qm", "init")
    _git(app, "push", "-q", "origin", "main")
    # A gitignored upstream snapshot: the restore must leave it alone.
    (app / ".git" / "info" / "exclude").write_text("data/*.xls\n")
    (app / "data" / "list.xls").write_text("snapshot")
    log.mkdir()
    stub = tmp_path / "python"
    stub.write_text(STUB)
    stub.chmod(0o755)

    def run(new_summary):
        env = os.environ | {"APP_DIR": str(app), "PYTHON": str(stub), "RUN_TESTS": "0",
                            "PUSH_CHANGES": "0", "STUB_LOG": str(log), "NEW_SUMMARY": new_summary,
                            "GIT_AUTHOR_NAME": "bot", "GIT_AUTHOR_EMAIL": "bot@example.invalid"}
        return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)

    return {"app": app, "log": log, "run": run}


def _leave_failed_run_behind(app):
    (app / "outputs" / "summary.csv").write_text(NEW)
    (app / "data" / "x.feather").write_text("edited by a failed run")
    (app / "outputs" / "stray.xlsx").write_text("left over")
    (app / "data" / "stray.feather").write_text("left over")


def test_leftover_summary_is_not_the_baseline(lane):
    app = lane["app"]
    _leave_failed_run_behind(app)
    run = lane["run"](NEW)  # the next run produces the same new data
    assert run.returncode == 0, run.stderr
    assert "No canonical summary.csv changes" not in run.stdout
    assert _git(app, "show", "HEAD:outputs/summary.csv") == NEW


def test_run_starts_from_committed_outputs_not_leftovers(lane):
    app, log = lane["app"], lane["log"]
    _leave_failed_run_behind(app)
    run = lane["run"](NEW)
    assert run.returncode == 0, run.stderr
    # The pipeline reads the committed summary and cache, not the edited ones ...
    assert (log / "input").read_text() == COMMITTED
    assert (log / "feather").read_text() == "committed"
    # ... only its own outputs are published, and the strays are gone ...
    published = _git(app, "show", "--name-only", "--format=", "HEAD").split()
    assert published == ["outputs/summary.csv"]
    assert not (app / "outputs" / "stray.xlsx").exists()
    assert not (app / "data" / "stray.feather").exists()
    # ... while gitignored upstream snapshots in data/ survive.
    assert (app / "data" / "list.xls").read_text() == "snapshot"


def test_unchanged_data_still_publishes_nothing(lane):
    run = lane["run"](COMMITTED)
    assert run.returncode == 0, run.stderr
    assert "No canonical summary.csv changes" in run.stdout
    assert _git(lane["app"], "rev-list", "--count", "HEAD").strip() == "1"
