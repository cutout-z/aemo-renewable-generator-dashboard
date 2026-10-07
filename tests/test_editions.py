"""ELI and ISP editions travel with the data, and a mix of editions fails validation (S2-3)."""

import pandas as pd
import pytest

from src import eli_appendix as ea
from src import source_status
from src.download_rez import fetch_rez_forecasts
from src.merge import build_summary
from test_eli_appendix import APPENDIX
from test_validate_outputs import _run, _status, _summary


@pytest.mark.parametrize("texts, expected", [
    (["forecasts from the 2024 ISP Step Change scenario", "Final 2024 ISP"], "2024 ISP"),
    (["the 2026 Integrated System Plan"], "2026 ISP"),
    (["no edition named here"], None),
    # two ISPs cited: not guessed between, the caller passes --isp-edition
    (["forecast from the 2024 ISP; the 2026 ISP will update it"], None),
])
def test_parse_isp_edition(texts, expected):
    assert ea.parse_isp_edition(texts) == expected


def _texts(tmp_path, extra=""):
    paths = []
    for prefix in ("a3", "a4", "a5", "a6", "a7"):
        p = tmp_path / f"{prefix}-appendix.txt"
        p.write_text(APPENDIX + extra)
        paths.append(str(p))
    return paths


def test_appendix_build_stamps_both_files(tmp_path):
    texts = _texts(tmp_path, "\nThe forecasts are from the 2024 ISP (Step Change).\n")
    assert ea.main(texts + ["--cache-dir", str(tmp_path), "--year", "2025"]) == 0
    for name in ("rez_forecasts.feather", "rez_membership.feather"):
        df = pd.read_feather(tmp_path / name)
        assert ea.editions(df) == {"eli_edition": 2025, "isp_edition": "2024 ISP"}, name


def test_appendix_build_refuses_without_an_isp_edition(tmp_path):
    assert ea.main(_texts(tmp_path) + ["--cache-dir", str(tmp_path), "--year", "2025"]) == 1
    assert not (tmp_path / "rez_forecasts.feather").exists()
    assert ea.main(_texts(tmp_path) + ["--cache-dir", str(tmp_path), "--year", "2025",
                                       "--isp-edition", "2024 ISP"]) == 0


def test_rez_load_records_the_files_own_editions(tmp_path):
    membership, forecasts = ea.parse_appendix_text(APPENDIX, "NSW")
    ea.stamp_editions(forecasts, 2025, "2024 ISP").to_feather(tmp_path / "rez_forecasts.feather")
    membership.to_feather(tmp_path / "rez_membership.feather")  # unstamped
    fetch_rez_forecasts(str(tmp_path))
    rec = source_status.load(tmp_path)["rez"]
    assert rec["forecasts_eli_edition"] == 2025 and rec["isp_edition"] == "2024 ISP"
    assert rec["membership_eli_edition"] is None


def test_summary_carries_the_editions(tmp_path):
    gens = pd.DataFrame({"DUID": ["ARSF1"], "FUEL_TYPE": ["Solar"], "LOCATION": ["Ararat"],
                         "VOLTAGE_KV": [220], "STATE": ["VIC"], "REGIONID": ["VIC1"],
                         "PROJECT_NAME": ["Ararat Solar"], "NAMEPLATE_MW": [100.0],
                         "REZ_NAME": ["New England"]})
    eli = pd.DataFrame({"LOCATION": ["Ararat"], "VOLTAGE_KV": [220], "REGION": ["VIC"],
                        "SOLAR_CURTAILMENT_NEAR": [0.46], "WIND_CURTAILMENT_NEAR": [0.33],
                        "SOLAR_CURTAILMENT_MED": [0.06], "WIND_CURTAILMENT_MED": [0.05],
                        "ELI_EDITION": [2025]})
    _, forecasts = ea.parse_appendix_text(APPENDIX, "NSW")
    rez = ea.stamp_editions(forecasts, 2025, "2024 ISP")
    out = build_summary(gens, pd.DataFrame(), eli, rez, pd.DataFrame(), cache_dir=str(tmp_path))
    assert out.loc[0, "ELI_EDITION"] == 2025 and out.loc[0, "ISP_EDITION"] == "2024 ISP"
    # an unstamped table leaves the edition empty, which the validator then fails
    out = build_summary(gens, pd.DataFrame(), eli.drop(columns="ELI_EDITION"), forecasts,
                        pd.DataFrame(), cache_dir=str(tmp_path))
    assert pd.isna(out.loc[0, "ELI_EDITION"]) and out.loc[0, "ISP_EDITION"] == ""


def _rez(forecasts=2025, membership=2025):
    return {"forecasts_eli_edition": forecasts, "membership_eli_edition": membership,
            "isp_edition": "2024 ISP"}


def test_validator_fails_when_the_appendices_lag_the_chart_data(tmp_path):
    # config bumped to ELI 2026 (probe record and published column) without an appendix rebuild
    df = _summary().assign(ELI_EDITION=2026)
    status = _status() | {"eli": {"edition": 2026, "newer_edition_available": False,
                                  "checked_at": source_status.now_iso()}}
    errs = _run(tmp_path, df, status)
    assert any("rez_forecasts.feather is from the ELI 2025 appendices but the ELI chart data "
               "is 2026" in e for e in errs), errs
    assert any("rez_membership.feather" in e for e in errs), errs


def test_validator_fails_on_a_stale_cached_chart_feather(tmp_path):
    # config says 2026 but the run fell back to a cached 2025 chart feather
    status = _status() | {"rez": _rez(2026, 2026),
                          "eli": {"edition": 2026, "newer_edition_available": False,
                                  "checked_at": source_status.now_iso()}}
    errs = _run(tmp_path, _summary(), status)
    assert any("chart-data editions disagree" in e for e in errs), errs


def test_validator_fails_on_unstamped_appendix_files(tmp_path):
    errs = _run(tmp_path, _summary(), _status() | {"rez": _rez(membership=None)})
    assert any("rez_membership.feather records no ELI edition" in e for e in errs), errs


@pytest.mark.parametrize("col, expected", [("ELI_EDITION", "carry no ELI_EDITION"),
                                           ("ISP_EDITION", "carry no ISP_EDITION")])
def test_validator_fails_on_values_without_an_edition(tmp_path, col, expected):
    df = _summary()
    df["ISP_CURTAILMENT_FY1"] = [0.1] + [float("nan")] * (len(df) - 1)
    df[col] = ""
    assert any(expected in e for e in _run(tmp_path, df, _status())), col


def test_validator_passes_matching_editions(tmp_path):
    status = _status() | {"eli": {"edition": 2025, "newer_edition_available": False,
                                  "checked_at": source_status.now_iso()}}
    assert _run(tmp_path, _summary(), status) == []
