"""The workbook seed never overwrites caches silently, and never the ELI-appendix files."""

import pandas as pd
import pytest

from src import seed_from_workbook as sw

SEEDED = ["eli_curtailment.feather", "generator_enrichment.feather", "eli_per_duid.feather"]


def _workbook(path):
    eli = pd.DataFrame([["Location", "Voltage (kV)", "Region ", "Solar Projected Curtailment",
                         "Wind Projected Curtailment"], ["Ararat", 220, "VIC", 0.46, 0.33]])
    summary = pd.DataFrame([
        ["Project name", "DUID", "Location", "REZ (Y/N)", "REZ", "State",
         "Nameplate capacity (MW)", "Voltage (kV)", "Near term", "Medium term"],
        ["Aaa Solar Farm", "AAASF1", "Ararat", "Y", "Western Victoria", "VIC", 100, 220, 0.46, 0.06],
    ])
    with pd.ExcelWriter(path, engine="openpyxl") as xw:
        for sheet in ("Near Term Proj Curtailment", "Med Term Proj Curtailment"):
            pd.DataFrame([["title"]]).to_excel(xw, sheet_name=sheet, index=False, header=False)
            eli.to_excel(xw, sheet_name=sheet, index=False, header=False, startrow=1)
        summary.to_excel(xw, sheet_name="Summary", index=False, header=False)
    return path


def _sentinel(path):
    pd.DataFrame({"SENTINEL": [1]}).to_feather(path)


def _is_sentinel(path):
    return list(pd.read_feather(path).columns) == ["SENTINEL"]


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A stand-in project root, so the default target (data/) is never the real one."""
    monkeypatch.setattr(sw, "PROJECT_ROOT", tmp_path)
    (tmp_path / "data").mkdir()
    return tmp_path


def test_default_run_does_not_overwrite_existing_caches(repo, tmp_path):
    wb = _workbook(tmp_path / "databook.xlsx")
    for name in SEEDED:
        _sentinel(repo / "data" / name)
    with pytest.raises(SystemExit):
        sw.seed(str(wb))
    assert all(_is_sentinel(repo / "data" / name) for name in SEEDED)


def test_out_dir_writes_there_and_leaves_data_alone(repo, tmp_path):
    wb = _workbook(tmp_path / "databook.xlsx")
    _sentinel(repo / "data" / "eli_per_duid.feather")
    sw.seed(str(wb), out_dir=tmp_path / "seed")
    assert all((tmp_path / "seed" / name).exists() for name in SEEDED)
    assert _is_sentinel(repo / "data" / "eli_per_duid.feather")


def test_force_replaces_the_seeded_files_but_never_the_appendix_files(repo, tmp_path):
    wb = _workbook(tmp_path / "databook.xlsx")
    for name in SEEDED + sorted(sw.PROTECTED):
        _sentinel(repo / "data" / name)
    sw.seed(str(wb), force=True)
    assert not any(_is_sentinel(repo / "data" / name) for name in SEEDED)
    assert all(_is_sentinel(repo / "data" / name) for name in sw.PROTECTED)


def test_protected_names_are_refused():
    assert sw.PROTECTED == {"rez_forecasts.feather", "rez_membership.feather"}
    for name in sw.PROTECTED:
        with pytest.raises(ValueError):
            sw._write(pd.DataFrame({"x": [1]}), sw.PROJECT_ROOT / "data" / name)
