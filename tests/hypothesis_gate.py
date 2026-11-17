"""The one place the fixed Hypothesis profile is registered and loaded.

Property modules do `import hypothesis_gate` (after `pytest.importorskip("hypothesis")`) so every run
is reproducible: derandomized, no example database (so no `.hypothesis/` directory), 100 examples.
"""

import tempfile

from hypothesis import HealthCheck, settings
from hypothesis.configuration import set_hypothesis_home_dir

# Keep any Hypothesis scratch state out of the worktree (no `.hypothesis/` directory).
set_hypothesis_home_dir(tempfile.mkdtemp(prefix="hypothesis-"))

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
