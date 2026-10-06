"""ISP forecasts are labelled with the ISP they come from, and an ended year as ended (L8)."""

import re
from pathlib import Path

from src import config

ROOT = Path(__file__).resolve().parent.parent
PAGE = (ROOT / "index.html").read_text(encoding="utf-8")


def test_group_labels_name_the_isp_edition():
    labels = dict(re.findall(r"'(isp-[co])':'([^']+)'", PAGE))
    assert all(config.ISP_FORECAST_EDITION in v for v in labels.values()), labels
    # the export's headers start with the group label; keep the family name first
    assert labels["isp-c"].startswith("ISP curtailment forecast")
    assert labels["isp-o"].startswith("ISP offloading forecast")


def test_an_ended_year_is_marked_on_the_page():
    assert "function fyEnded" in PAGE and "That year has ended" in PAGE
    assert 'title="${d.title}"' in PAGE
    # 1 July 00:00 AEST is 30 June 14:00 UTC
    assert "Date.UTC(2000 + parseInt(m[2], 10), 5, 30, 14)" in PAGE


def test_readme_names_the_source_and_the_ended_year():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "Final 2024 ISP" in readme and "has since ended" in readme
    assert "Next 3 FY forecasts" not in readme
