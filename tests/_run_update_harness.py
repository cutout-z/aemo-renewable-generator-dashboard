"""Shared harness for tests of deploy/run-update.sh.

No network: a local bare origin stands in for GitHub and a stub PYTHON (a bash script that switches on
the `-m <module>` argument) stands in for the pipeline and the post-publish check.
"""

import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "deploy" / "run-update.sh"

COMMITTED = "DUID\nA\n"


def git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout


def make_lane(tmp_path, stub_text):
    """A clone of a bare origin holding committed outputs/summary.csv and data/x.feather, plus a stub PYTHON.

    A gitignored upstream snapshot (data/list.xls) sits in data/, which the script must leave alone.
    Returns {"origin", "app", "stub"} as paths.
    """
    origin, app = tmp_path / "origin.git", tmp_path / "app"
    git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    git(tmp_path, "clone", "-q", str(origin), str(app))
    git(app, "checkout", "-q", "-b", "main")
    (app / "outputs").mkdir()
    (app / "data").mkdir()
    (app / "outputs" / "summary.csv").write_text(COMMITTED)
    (app / "data" / "x.feather").write_text("committed")
    git(app, "add", ".")
    git(app, "commit", "-qm", "init")
    git(app, "push", "-q", "origin", "main")
    (app / ".git" / "info" / "exclude").write_text("data/*.xls\n")
    (app / "data" / "list.xls").write_text("snapshot")
    stub = tmp_path / "python"
    stub.write_text(stub_text)
    stub.chmod(0o755)
    return {"origin": origin, "app": app, "stub": stub}


def run_script(lane, **env):
    """Run deploy/run-update.sh against the lane; `env` is added to the base environment."""
    base = {"APP_DIR": str(lane["app"]), "PYTHON": str(lane["stub"]), "RUN_TESTS": "0"}
    return subprocess.run(["bash", str(SCRIPT)], env=os.environ | base | env,
                          capture_output=True, text=True)
