"""ELI: per-DUID first, then the location table (fuel-matched), with ELI_SOURCE."""

import pandas as pd

from src.download_eli import combine_terms
from src.merge import build_summary


def _eli_table():
    return pd.DataFrame([
        # LOCATION, VOLTAGE_KV, REGION, SOLAR_NEAR, WIND_NEAR, SOLAR_MED, WIND_MED
        ("Ararat", 220, "VIC", 0.46, 0.33, 0.06, 0.059),
        ("Armidale", 132, "NSW", 0.229, 0.096, 0.051, 0.031),
        ("Armidale", 330, "NSW", 0.231, 0.098, 0.052, 0.032),
        ("Mobilong", 3, "SA", 0.90, 0.80, 0.70, 0.60),
        ("Mobilong", 3.3, "SA", 0.51, 0.40, 0.30, 0.20),
        ("Deniliquin", 132, "NSW", 0.70, 0.18, 0.26, 0.05),
    ], columns=["LOCATION", "VOLTAGE_KV", "REGION", "SOLAR_CURTAILMENT_NEAR",
                "WIND_CURTAILMENT_NEAR", "SOLAR_CURTAILMENT_MED", "WIND_CURTAILMENT_MED"])


def _gens(rows):
    df = pd.DataFrame(rows, columns=["DUID", "FUEL_TYPE", "LOCATION", "VOLTAGE_KV", "STATE"])
    df["PROJECT_NAME"] = df["DUID"]
    df["REGIONID"] = df["STATE"] + "1"
    df["NAMEPLATE_MW"] = 100.0
    return df


def _summary(gens, tmp_path, per_duid=None):
    if per_duid is not None:
        per_duid.to_feather(tmp_path / "eli_per_duid.feather")
    return build_summary(gens, pd.DataFrame(), _eli_table(), pd.DataFrame(), pd.DataFrame(),
                         cache_dir=str(tmp_path)).set_index("DUID")


def test_wind_gets_wind_columns_and_solar_solar_columns(tmp_path):
    out = _summary(_gens([
        ("ARWF1", "Wind", "Ararat", 220, "VIC"),
        ("ARSF1", "Solar", "ararat ", 220, "VIC"),
    ]), tmp_path)
    assert out.loc["ARWF1", "ELI_CURTAILMENT_NEAR"] == 0.33
    assert out.loc["ARWF1", "ELI_CURTAILMENT_MED"] == 0.059
    assert out.loc["ARSF1", "ELI_CURTAILMENT_NEAR"] == 0.46
    assert set(out["ELI_SOURCE"]) == {"location"}


def test_per_duid_value_wins_and_is_labelled(tmp_path):
    per_duid = pd.DataFrame({"DUID": ["ARSF1"], "ELI_CURTAILMENT_NEAR": [0.12],
                             "ELI_CURTAILMENT_MED": [0.02]})
    out = _summary(_gens([
        ("ARSF1", "Solar", "Ararat", 220, "VIC"),
        ("ARWF1", "Wind", "Ararat", 220, "VIC"),
    ]), tmp_path, per_duid)
    assert out.loc["ARSF1", "ELI_CURTAILMENT_NEAR"] == 0.12
    assert out.loc["ARSF1", "ELI_SOURCE"] == "per-DUID"
    assert out.loc["ARWF1", "ELI_SOURCE"] == "location"


def test_voltage_is_not_truncated_to_int(tmp_path):
    out = _summary(_gens([("MAPS2PV1", "Solar", "Mobilong", 3.3, "SA")]), tmp_path)
    assert out.loc["MAPS2PV1", "ELI_CURTAILMENT_NEAR"] == 0.51  # the 3.3 kV row, not 3 kV


def test_several_voltages_without_a_match_stay_empty(tmp_path):
    out = _summary(_gens([
        ("METZSF1", "Solar", "Armidale", 66, "NSW"),
        ("NEWENSF1", "Solar", "Armidale", 330, "NSW"),
    ]), tmp_path)
    assert pd.isna(out.loc["METZSF1", "ELI_CURTAILMENT_NEAR"])
    assert out.loc["METZSF1", "ELI_SOURCE"] == ""
    assert out.loc["NEWENSF1", "ELI_CURTAILMENT_NEAR"] == 0.231


def test_location_must_be_in_the_units_region(tmp_path):
    out = _summary(_gens([
        ("FINLYSF1", "Solar", "Deniliquin", 132, "NSW"),
        ("ODD1", "Solar", "Deniliquin", 132, "SA"),
        ("NOLOC1", "Wind", None, None, "VIC"),
    ]), tmp_path)
    assert out.loc["FINLYSF1", "ELI_CURTAILMENT_NEAR"] == 0.70
    assert pd.isna(out.loc["ODD1", "ELI_CURTAILMENT_NEAR"])
    assert out.loc["NOLOC1", "ELI_SOURCE"] == ""
    assert set(out["ELI_SOURCE"]) <= {"per-DUID", "location", ""}


def test_combine_terms_does_not_split_a_location_across_regions():
    near = pd.DataFrame({"LOCATION": ["Deniliquin"], "VOLTAGE_KV": [132], "REGION": ["NSW"],
                         "SOLAR_CURTAILMENT_NEAR": [0.70], "WIND_CURTAILMENT_NEAR": [0.18]})
    med = pd.DataFrame({"LOCATION": ["Deniliquin"], "VOLTAGE_KV": [132], "REGION": ["SA"],
                        "SOLAR_CURTAILMENT_MED": [0.26], "WIND_CURTAILMENT_MED": [0.05]})
    out = combine_terms(near, med)
    assert len(out) == 1
    row = out.iloc[0]
    assert row["REGION"] == "NSW"
    assert row["SOLAR_CURTAILMENT_NEAR"] == 0.70 and row["SOLAR_CURTAILMENT_MED"] == 0.26
