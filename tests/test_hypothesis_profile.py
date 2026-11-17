"""Pins that the gate Hypothesis profile is the one in force (a property module that forgot it is caught)."""

import pytest

pytest.importorskip("hypothesis", reason="hypothesis is a dev-only dependency (requirements-dev.txt)")

import hypothesis_gate  # noqa: E402,F401
from hypothesis import settings  # noqa: E402


def test_gate_profile_is_loaded():
    assert settings().derandomize is True
    assert settings().database is None
