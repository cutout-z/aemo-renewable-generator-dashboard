"""ELI horizon labels agree everywhere, and with AEMO's 2025 ELI report.

The report's executive summary: projected curtailment "under near-term (2026 to
2028), and medium-term (2030 to 2035) horizons". The page said "Med (29-35)".
"""

import re
from pathlib import Path

from src import config

ROOT = Path(__file__).resolve().parent.parent


def test_config_states_aemos_horizons():
    assert config.ELI_HORIZONS == {"NEAR": (2026, 2028), "MED": (2030, 2035)}


def test_page_column_labels_match():
    page = (ROOT / "index.html").read_text(encoding="utf-8")
    labels = dict(re.findall(r"key:'ELI_CURTAILMENT_(NEAR|MED)', label:'(?:Near|Med) \((\d\d-\d\d)\)'", page))
    assert labels == {"NEAR": "26-28", "MED": "30-35"}
    assert "2026-28" in page and "29-35" not in page


def test_readme_matches():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Near-term (2026-28) and medium-term (2030-35)" in readme
    assert "29-35" not in readme and "2029-35" not in readme


def test_workbook_labels_carry_the_horizons():
    import pandas as pd
    from src.excel_output import _get_column_spec
    spec = dict(_get_column_spec(pd.DataFrame(columns=["DUID", "ELI_CURTAILMENT_NEAR", "ELI_CURTAILMENT_MED"])))
    assert spec["ELI_CURTAILMENT_NEAR"] == "ELI Near Term (2026-28)"
    assert spec["ELI_CURTAILMENT_MED"] == "ELI Med Term (2030-35)"
