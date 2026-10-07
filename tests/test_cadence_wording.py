"""Page and docs describe how each source really updates (S4-1, S4-3, S4-4, S4-5, S4-6)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAGE = (ROOT / "index.html").read_text(encoding="utf-8")
README = (ROOT / "README.md").read_text(encoding="utf-8")
DEPLOY = (ROOT / "deploy" / "README.md").read_text(encoding="utf-8")


def test_no_annual_july_claims_for_eli_isp_or_mlf():
    for text in (PAGE, README):
        assert "annual (July)" not in text and "Annual (July)" not in text
        assert "July/Oct" not in text
        assert "With each ELI edition (July)" not in text
    assert "updated by hand when AEMO publishes a new one" in PAGE
    assert "via the ${EDITIONS.eli} ELI appendices" in PAGE


def test_mlf_row_has_no_draft_column_claim():
    assert "current year draft" not in README
    assert "by about 1 April" in README and "by about 1 April" in PAGE


def test_no_claim_that_rez_workbooks_fall_back_in_the_lane():
    for text in (" ".join(README.split()), " ".join(DEPLOY.split())):
        assert "ELI/REZ workbook URLs fail" not in text
        assert "not fetched" in text and "python -m src.eli_appendix" in text


def test_eli_links_to_its_own_aemo_page():
    assert "forecasting-and-planning-data/enhanced-locational-information)" in README
    assert "inputs-assumptions-and-methodologies" not in README


def test_actuals_source_is_described_the_same_way_everywhere():
    for path in ("src/config.py", "src/fetch_curtailment.py"):
        text = (ROOT / path).read_text(encoding="utf-8")
        assert "AVAILABILITY in DISPATCHLOAD" in text, path
    assert "`AVAILABILITY` in `DISPATCHLOAD`" in README
