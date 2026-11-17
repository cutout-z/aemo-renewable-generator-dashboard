"""Pins that the gate Hypothesis profile is in force without this module loading it itself.

tests/conftest.py loads the profile, so a conftest that stops doing so is caught here.
"""

import pytest

pytest.importorskip("hypothesis", reason="hypothesis is a dev-only dependency (requirements-dev.txt)")

from hypothesis import settings  # noqa: E402


def test_gate_profile_is_loaded():
    assert settings.get_current_profile_name() == "gate"
    assert settings().derandomize is True
    assert settings().database is None
    assert settings().deadline is None
