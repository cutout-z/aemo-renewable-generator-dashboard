"""The one place the fixed Hypothesis profile is registered and loaded.

tests/conftest.py imports this module (when Hypothesis is installed), so every run is reproducible:
derandomized, no example database (so no `.hypothesis/` directory), 100 examples. Property modules
may also import it explicitly.
"""

import atexit
import shutil
import tempfile

from hypothesis import HealthCheck, settings
from hypothesis.configuration import set_hypothesis_home_dir

# Keep any Hypothesis scratch state out of the worktree (no `.hypothesis/` directory), and remove it at exit.
_HOME = tempfile.mkdtemp(prefix="hypothesis-")
atexit.register(shutil.rmtree, _HOME, ignore_errors=True)
set_hypothesis_home_dir(_HOME)

settings.register_profile(
    "gate",
    derandomize=True,
    max_examples=100,
    deadline=None,
    database=None,
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("gate")
