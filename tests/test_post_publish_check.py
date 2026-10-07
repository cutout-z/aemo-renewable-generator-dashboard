"""A confirmed newer ELI edition turns the lane red after publishing (decision 6(b)).

src.post_publish_check exits non-zero; deploy/run-update.sh runs it on every normal exit
path, after any push, so the run's other updates are still published.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from src import post_publish_check as ppc

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "deploy" / "run-update.sh"
NEWER = {"edition": 2025, "newer_edition_available": True,
         "probed_url": "https://x/2026/2026-eli-report-chart-data.xlsx"}


def _cache(tmp_path, eli):
    (tmp_path / ppc.STATUS_FILE).write_text(json.dumps({"eli": eli}))
    return str(tmp_path)


def test_fails_with_the_steps_when_a_newer_edition_is_confirmed(tmp_path, capsys):
    assert ppc.main(["--cache-dir", _cache(tmp_path, NEWER)]) == 1
    err = capsys.readouterr().err
    assert "ELI 2026 has been published" in err and "still uses ELI 2025" in err
    assert "ELI_CHART_DATA_URLS" in err and "ELI_REGIONAL_APPENDIX_URLS" in err
    assert "python -m src.eli_appendix" in err and "python -m src.main --full-refresh" in err


@pytest.mark.parametrize("eli", [
    {"edition": 2025, "newer_edition_available": False},
    {"edition": 2025, "newer_edition_available": None, "error": "HTTP 403"},  # blind: the validator's job
    {},
])
def test_passes_without_a_confirmed_newer_edition(tmp_path, eli):
    assert ppc.main(["--cache-dir", _cache(tmp_path, eli)]) == 0


def test_passes_without_a_status_file(tmp_path):
    assert ppc.main(["--cache-dir", str(tmp_path)]) == 0


def test_runs_as_a_module(tmp_path):
    run = subprocess.run(["python3", "-m", "src.post_publish_check", "--cache-dir",
                          _cache(tmp_path, NEWER)], cwd=ROOT, capture_output=True, text=True)
    assert run.returncode == 1 and "ELI 2026 has been published" in run.stderr


# ── deploy/run-update.sh ─────────────────────────────────────────────────

def test_every_normal_exit_of_the_wrapper_runs_the_check():
    script = SCRIPT.read_text()
    assert re.search(r'finish\(\) \{\n.*"\$\{PYTHON\}" -m src\.post_publish_check', script, re.S)
    assert "exit 0" not in script            # the early exits go through finish
    assert script.rstrip().endswith("finish")  # and so does the end, after the push


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


@pytest.fixture
def lane(tmp_path):
    """A clone with an origin, plus a stub PYTHON standing in for the pipeline and the check."""
    if not shutil.which("git"):
        pytest.skip("git not available")
    origin, app = tmp_path / "origin.git", tmp_path / "app"
    _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    _git(tmp_path, "clone", "-q", str(origin), str(app))
    (app / "outputs").mkdir()
    (app / "data").mkdir()
    (app / "outputs" / "summary.csv").write_text("DUID\nA\n")
    (app / "data" / "x.feather").write_bytes(b"x")
    _git(app, "-c", "user.name=t", "-c", "user.email=t@t", "checkout", "-q", "-b", "main")
    _git(app, "add", ".")
    _git(app, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "init")
    _git(app, "push", "-q", "origin", "main")
    stub = tmp_path / "python"
    stub.write_text('#!/usr/bin/env bash\n'
                    'case "$2" in\n'
                    '  src.main) [[ -n "${CHANGE:-}" ]] && echo B >> outputs/summary.csv; exit 0 ;;\n'
                    '  src.post_publish_check) echo ran > "${MARKER}"; exit "${CHECK_RC}" ;;\n'
                    'esac\n')
    stub.chmod(0o755)
    return {"origin": origin, "app": app, "stub": stub, "marker": tmp_path / "marker"}


def _run_lane(lane, check_rc, change):
    env = os.environ | {"APP_DIR": str(lane["app"]), "PYTHON": str(lane["stub"]), "RUN_TESTS": "0",
                        "MARKER": str(lane["marker"]), "CHECK_RC": str(check_rc),
                        "CHANGE": "1" if change else ""}
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True)


@pytest.mark.parametrize("check_rc", [0, 1])
def test_no_change_exit_runs_the_check(lane, check_rc):
    run = _run_lane(lane, check_rc, change=False)
    assert "No canonical summary.csv changes" in run.stdout, run.stderr
    assert lane["marker"].exists()
    assert run.returncode == check_rc


@pytest.mark.parametrize("check_rc", [0, 1])
def test_check_runs_after_the_push(lane, check_rc):
    run = _run_lane(lane, check_rc, change=True)
    assert lane["marker"].exists(), run.stderr
    assert run.returncode == check_rc
    # the update was published even though the lane is red
    pushed = subprocess.run(["git", "--git-dir", str(lane["origin"]), "show", "main:outputs/summary.csv"],
                            capture_output=True, text=True).stdout
    assert pushed == "DUID\nA\nB\n"
