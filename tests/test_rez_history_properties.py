"""Property test: rez_history.append never rewrites earlier rows, over random edition sequences.

Dev-only (Hypothesis); skipped visibly where it is not installed.
"""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("hypothesis", reason="hypothesis is a dev-only dependency (requirements-dev.txt)")

import hypothesis_gate  # noqa: E402,F401  (loads the fixed profile)
from hypothesis import given, strategies as st  # noqa: E402

from src import rez_history  # noqa: E402

EDITION_POOL = [("ELI 2025 regional appendices", "2025", "2024 ISP"),
                ("ELI 2026 regional appendices", "2026", "2026 ISP"),
                ("2026 ISP Appendix A3", "", "2026 ISP"),
                ("2028 ISP Appendix A3", "", "2028 ISP")]
ZONES = [("N1", "Zone one"), ("V3", "Zone, with comma"), ("", 'Zone "quoted"')]
FYS = ["2025-26", "2026-27", "2027-28"]
values = st.one_of(st.just(""), st.floats(min_value=0, max_value=1, allow_nan=False).map(lambda v: repr(round(v, 6))))


@st.composite
def edition_rows(draw):
    source, eli, isp = draw(st.sampled_from(EDITION_POOL))
    zones = draw(st.lists(st.sampled_from(ZONES), min_size=1, max_size=3, unique=True))
    measures = draw(st.lists(st.sampled_from(["curtailment", "offloading"]), min_size=1, max_size=2, unique=True))
    retrieved = draw(st.sampled_from(["2026-10-06", "2026-10-07", "2027-01-01"]))
    rows = []
    for rez_id, name in zones:
        for measure in measures:
            for fy in FYS:
                rows.append({"source": source, "eli_edition": eli, "isp_edition": isp, "rez_id": rez_id,
                             "rez_name": name, "state": "NSW", "scenario": "Step Change", "fy": fy,
                             "measure": measure, "value": draw(values), "crosswalk_key": name,
                             "crosswalk_match": "exact", "retrieved_at": retrieved})
    return (source, eli, isp), pd.DataFrame(rows, columns=rez_history.COLUMNS)


def _figures(df):
    return sorted(df[[c for c in rez_history.COLUMNS if c != "retrieved_at"]].itertuples(index=False, name=None))


@st.composite
def append_calls(draw):
    """One append call: a single edition's rows, or (sometimes) two distinct editions concatenated."""
    first = draw(edition_rows())
    if draw(st.booleans()):
        second = draw(edition_rows())
        if second[0] != first[0]:
            return [first, second]
    return [first]


@given(calls=st.lists(append_calls(), min_size=1, max_size=8))
def test_append_never_rewrites_earlier_rows(calls):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "history.csv"
        held = {}  # edition key -> figures, the model of what is on file
        for call in calls:
            before = path.read_bytes() if path.exists() else b""
            rows = pd.concat([r for _, r in call], ignore_index=True)
            conflict = any(k in held and held[k] != _figures(r) for k, r in call)
            added = sum(len(r) for k, r in call if k not in held)
            if conflict:
                with pytest.raises(rez_history.HistoryConflict, match="append-only"):
                    rez_history.append(path, rows)
                added = 0
            else:
                assert rez_history.append(path, rows) == added
                for k, r in call:
                    held.setdefault(k, _figures(r))
            after = path.read_bytes() if path.exists() else b""
            assert after.startswith(before)                          # earlier bytes never rewritten
            if conflict:
                assert after == before                               # a refused call writes nothing
            if before:
                assert after.split(b"\n", 1)[0] == before.split(b"\n", 1)[0]  # one header, still first
            assert rez_history.editions(rez_history.load(path)) == set(held)
            assert len(rez_history.load(path)) == sum(len(f) for f in held.values())
