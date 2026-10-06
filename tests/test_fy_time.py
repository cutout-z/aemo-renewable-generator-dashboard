"""The FY rollover is read in NEM time (AEST, UTC+10), not the host's clock."""

from datetime import datetime, timezone

from src import config


class _UtcHostClock(datetime):
    """A host whose local zone is UTC (the GitHub runner), at 2026-06-30 15:00 UTC,
    which is already 2026-07-01 01:00 in the NEM."""

    INSTANT = datetime(2026, 6, 30, 15, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.INSTANT.replace(tzinfo=None)  # naive host-local time
        return cls.INSTANT.astimezone(tz)


def test_rollover_follows_nem_time_not_a_utc_host(monkeypatch):
    monkeypatch.setattr(config, "datetime", _UtcHostClock)
    assert config.current_fy_start() == 2026  # FY26-27 has begun in the NEM


def test_explicit_instants():
    assert config.current_fy_start(datetime(2026, 6, 30, 13, 59, tzinfo=timezone.utc)) == 2025
    assert config.current_fy_start(datetime(2026, 6, 30, 14, 0, tzinfo=timezone.utc)) == 2026
    assert config.current_fy_start(datetime(2026, 3, 1)) == 2025
