"""A trimmed source status is published next to summary.csv and shown in the page footer (S3-1)."""

import json
from pathlib import Path

from src import source_status

ROOT = Path(__file__).resolve().parent.parent

FULL = {
    "registration_list": {"fetched_at": "2026-10-06T01:42:44+00:00", "refreshed": True,
                          "error": None, "url": "https://x/list.xls", "file": "list.xls"},
    "eli": {"edition": 2025, "checked_at": "2026-10-06T01:42:48+00:00", "error": None,
            "newer_edition_available": False, "probed_url": "https://x/2026.xlsx",
            "probe_status": 302, "last_conclusive_check": "2026-10-06T01:42:48+00:00"},
    "mlf": {"newest_fy": "FY26-27", "fetched_at": "2026-10-06T01:00:00+00:00",
            "refreshed": False, "used_cache": True, "error": "x" * 500},
    "dudetailsummary": {"missing_from_registration": [{"DUID": "KIDSPHG2"}]},
}


def test_trimmed_keeps_editions_and_dates_only():
    out = source_status.trimmed(FULL)
    assert "dudetailsummary" not in out
    assert "url" not in out["registration_list"] and "probed_url" not in out["eli"]
    assert out["eli"]["edition"] == 2025 and out["eli"]["last_conclusive_check"]
    assert out["mlf"]["newest_fy"] == "FY26-27" and out["mlf"]["used_cache"] is True
    assert len(out["mlf"]["error"]) == source_status.MAX_ERROR_CHARS


def test_publish_writes_next_to_the_outputs(tmp_path):
    cache, out = tmp_path / "cache", tmp_path / "out"
    cache.mkdir()
    (cache / source_status.STATUS_FILE).write_text(json.dumps(FULL))
    path = source_status.publish(cache, out)
    assert path == out / "source_status.json"
    assert json.loads(path.read_text())["registration_list"]["refreshed"] is True


def test_page_footer_renders_the_published_status():
    page = (ROOT / "index.html").read_text(encoding="utf-8")
    assert "fetch('outputs/source_status.json'" in page and "renderSourceStatus();" in page
    assert "Sources at the last publish" in page   # the dates are of the last publish, not run


def test_lane_commits_the_status_file_with_outputs():
    script = (ROOT / "deploy" / "run-update.sh").read_text()
    assert "git add outputs/ data/*.feather" in script and "source_status.json" in script
    assert "cmp -s" in script   # still published only when summary.csv changes
    gitignore = (ROOT / ".gitignore").read_text()
    assert "outputs/*.json" not in gitignore and "outputs/" not in gitignore.split()
