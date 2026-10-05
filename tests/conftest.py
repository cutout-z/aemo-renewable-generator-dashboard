"""Shared pytest setup: make `src` importable and keep every test offline."""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
TESTS = Path(__file__).resolve().parent
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Fail loudly if a test reaches for the network instead of a fixture."""
    import requests

    def _blocked(*args, **kwargs):
        raise AssertionError("tests must not touch the network")

    monkeypatch.setattr(requests, "get", _blocked)
    monkeypatch.setattr(requests, "head", _blocked)
    monkeypatch.setattr(requests.Session, "request", _blocked)
