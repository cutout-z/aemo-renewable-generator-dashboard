"""The validator passes a well-formed run and fails each data gap it is meant to catch."""

import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import validate_outputs as vo

REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]


def _summary():
    rows = []
    for i in range(100):
        region = REGIONS[i % 5]
        wind = i % 2 == 1
        rows.append({
            "DUID": f"U{i:03d}", "PROJECT_NAME": f"Farm {i}", "REGIONID": region,
            "STATE": region[:-1], "FUEL_TYPE": "Wind" if wind else "Solar",
            "TECHNOLOGY": "Wind - Onshore" if wind else "Photovoltaic Flat panel",
            "NAMEPLATE_MW": 100.0,
            "REZ": "", "REZ_NAME": "", "REZ_SOURCE": "",
            "ELI_CURTAILMENT_NEAR": float("nan"), "ELI_CURTAILMENT_MED": float("nan"),
            "ELI_SOURCE": "", "ELI_EDITION": 2025, "ISP_EDITION": "2024 ISP",
        })
    df = pd.DataFrame(rows)
    df.loc[0, ["REZ", "REZ_NAME", "REZ_SOURCE"]] = ["Y", "Darling Downs", "seed"]
    df.loc[2, ["REZ", "REZ_NAME", "REZ_SOURCE"]] = ["N", "Non-REZ", "seed"]
    df.loc[0, ["ELI_CURTAILMENT_NEAR", "ELI_CURTAILMENT_MED", "ELI_SOURCE"]] = [0.2, 0.1, "location-name"]
    df.loc[1, ["ELI_CURTAILMENT_NEAR", "ELI_CURTAILMENT_MED", "ELI_SOURCE"]] = [0.3, 0.1, "location"]
    return df


def _status(reg_age_days=1, missing=()):
    fetched = (datetime.now(timezone.utc) - timedelta(days=reg_age_days)).isoformat()
    return {
        "registration_list": {"fetched_at": fetched, "refreshed": True, "error": None},
        "dudetailsummary": {"month": "2026-08", "missing_from_registration":
                            [{"DUID": d} for d in missing]},
        "gen_info": {"edition": "2026-07", "edition_age_days": 96},
        "rez": {"forecasts_eli_edition": 2025, "membership_eli_edition": 2025,
                "isp_edition": "2024 ISP"},
    }


def _run(tmp_path, df, status):
    out, cache = tmp_path / "out", tmp_path / "cache"
    out.mkdir(exist_ok=True)
    cache.mkdir(exist_ok=True)
    df.to_csv(out / "summary.csv", index=False)
    for name in ["NSW", "QLD", "VIC", "SA", "TAS"]:
        (out / f"{name}_curtailment.xlsx").write_bytes(b"x")
    if status is not None:
        (cache / vo.STATUS_FILE).write_text(json.dumps(status))
    vo.errors.clear()
    summary = vo.validate(out)
    vo.validate_sources(cache, summary)
    found = list(vo.errors)
    vo.errors.clear()
    return found


def test_good_run_passes(tmp_path):
    assert _run(tmp_path, _summary(), _status(missing=["KIDSPHG2"])) == []


def _mutate(df, idx, **cols):
    df = df.copy()
    for k, v in cols.items():
        df.loc[idx, k] = v
    return df


@pytest.mark.parametrize("change, expected", [
    # The published bug: unknown units labelled outside a REZ with no source
    (dict(REZ="N", REZ_NAME="Non-REZ", REZ_SOURCE=""), "no REZ_SOURCE"),
    (dict(REZ="", REZ_NAME="Non-REZ"), "without REZ = N"),
    (dict(REZ="Maybe"), "REZ has values outside"),
    (dict(REZ="Y", REZ_NAME="", REZ_SOURCE="seed"), "without a zone name"),
    (dict(REZ="N", REZ_NAME="Isaac", REZ_SOURCE="seed"), "REZ_NAME is not"),
    (dict(REZ_SOURCE="guess"), "REZ_SOURCE has unexpected"),
    (dict(ELI_CURTAILMENT_NEAR=0.4), "ELI value but no ELI_SOURCE"),
    (dict(ELI_SOURCE="nearby"), "ELI_SOURCE has unexpected"),
    # the hand-seeded per-DUID values are no longer a source (S2-2)
    (dict(ELI_CURTAILMENT_NEAR=0.4, ELI_SOURCE="per-DUID"), "ELI_SOURCE has unexpected"),
    (dict(TECHNOLOGY="Renewable"), None),  # one row is fine
])
def test_contract_violations_fail(tmp_path, change, expected):
    errs = _run(tmp_path, _mutate(_summary(), 4, **change), _status())
    if expected is None:
        assert errs == []
    else:
        assert any(expected in e for e in errs), errs


def test_every_technology_renewable_fails(tmp_path):
    df = _summary()
    df["TECHNOLOGY"] = "Renewable"
    assert any("Renewable" in e for e in _run(tmp_path, df, _status()))


def test_missing_rez_source_column_fails(tmp_path):
    errs = _run(tmp_path, _summary().drop(columns=["REZ_SOURCE"]), _status())
    assert any("Missing column: REZ_SOURCE" in e for e in errs)


def test_stale_registration_list_fails(tmp_path):
    errs = _run(tmp_path, _summary(), _status(reg_age_days=200))
    assert any("Registration List is 200 days old" in e for e in errs)


def test_many_unlisted_new_generators_fail(tmp_path):
    errs = _run(tmp_path, _summary(), _status(missing=["WANDSF2", "A1", "B1", "C1"]))
    assert any("WANDSF2" in e for e in errs)


def test_missing_status_file_fails(tmp_path):
    errs = _run(tmp_path, _summary(), None)
    assert any("source_status.json missing" in e for e in errs)
