"""REZ is Y / N / unknown, and "Non-REZ" only when a source says so."""

import pandas as pd
import pytest

from src import download_generators as dg
from src.merge import build_summary
from fixtures import registration_xlsx


@pytest.mark.parametrize("value, expected", [
    ("Darling Downs", ("Y", "Darling Downs")),
    ("  Isaac ", ("Y", "Isaac")),
    ("Non-REZ", ("N", "Non-REZ")),
    ("non rez", ("N", "Non-REZ")),
    (None, None), (float("nan"), None), ("", None), ("-", None), ("N/A", None),
])
def test_classify_rez_value(value, expected):
    assert dg.classify_rez_value(value) == expected


def _gens(*duids):
    return pd.DataFrame({"DUID": list(duids), "FUEL_TYPE": ["Solar"] * len(duids)})


def test_assign_rez_three_states_and_sources():
    seed = pd.DataFrame({
        "DUID": ["INREZ1", "OUTREZ1", "BOTH1", "NOFLAG1"],
        "REZ": ["Y", "N", "N", None],
        "REZ_NAME": ["Darling Downs", "Non-REZ", "Non-REZ", "Wide Bay"],
    })
    gen_info = pd.DataFrame({"DUID": ["BOTH1", "GIOUT1", "INREZ1"],
                             "REZ_NAME": ["Isaac", "Non-REZ", None]})
    out = dg.assign_rez(_gens("INREZ1", "OUTREZ1", "BOTH1", "NOFLAG1", "GIOUT1", "WIND1"),
                        gen_info=gen_info, seed=seed).set_index("DUID")

    assert tuple(out.loc["INREZ1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("Y", "Darling Downs", "seed")
    assert tuple(out.loc["OUTREZ1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("N", "Non-REZ", "seed")
    # Generation Information, where it states a REZ, wins over the seed
    assert tuple(out.loc["BOTH1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("Y", "Isaac", "geninfo")
    assert tuple(out.loc["NOFLAG1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("Y", "Wide Bay", "seed")
    assert tuple(out.loc["GIOUT1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("N", "Non-REZ", "geninfo")
    # No source at all: unknown, never "Non-REZ"
    assert tuple(out.loc["WIND1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("", "", "")


def test_seed_flag_y_without_a_zone_name_is_unknown():
    seed = pd.DataFrame({"DUID": ["X1"], "REZ": ["Y"], "REZ_NAME": [None]})
    out = dg.assign_rez(_gens("X1"), seed=seed).iloc[0]
    assert (out["REZ"], out["REZ_NAME"], out["REZ_SOURCE"]) == ("", "", "")


def test_fetch_generators_leaves_unseeded_wind_unknown(tmp_path, monkeypatch):
    (tmp_path / dg.REGISTRATION_FILE).write_bytes(registration_xlsx())
    monkeypatch.setattr(dg, "_fetch_bytes", lambda url: registration_xlsx())
    monkeypatch.setattr(dg, "_load_gen_info", lambda cache_path: None)
    monkeypatch.setattr(dg, "_cross_check_dudetailsummary", lambda *a, **k: None)
    pd.DataFrame({
        "DUID": ["WANDSF1"], "PROJECT_NAME": ["Wandoan Solar Farm"], "LOCATION": ["Columboola"],
        "REZ": ["Y"], "REZ_NAME": ["Darling Downs"], "STATE": ["QLD"],
        "NAMEPLATE_MW": [125.0], "VOLTAGE_KV": [275.0],
    }).to_feather(tmp_path / "generator_enrichment.feather")

    gens = dg.fetch_generators(str(tmp_path)).set_index("DUID")

    assert tuple(gens.loc["WANDSF1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("Y", "Darling Downs", "seed")
    assert tuple(gens.loc["ARWF1", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("", "", "")
    assert tuple(gens.loc["WANDSF2", ["REZ", "REZ_NAME", "REZ_SOURCE"]]) == ("", "", "")
    assert (gens["REZ_NAME"] == "Non-REZ").sum() == 0


def test_isp_forecasts_join_only_named_zones(tmp_path):
    gens = pd.DataFrame({
        "DUID": ["IN1", "OUT1", "UNK1"], "PROJECT_NAME": ["a", "b", "c"],
        "REZ": ["Y", "N", ""], "REZ_NAME": ["Darling Downs", "Non-REZ", ""],
        "REZ_SOURCE": ["seed", "seed", ""], "STATE": ["QLD"] * 3, "REGIONID": ["QLD1"] * 3,
        "NAMEPLATE_MW": [1.0] * 3, "FUEL_TYPE": ["Solar"] * 3,
    })
    rez = pd.DataFrame({"STATE": ["QLD"], "REZ_NAME": ["Darling Downs"],
                        "CURTAILMENT_FY1": [0.05], "CURTAILMENT_AVG": [0.04]})
    out = build_summary(gens, pd.DataFrame(), pd.DataFrame(), rez, pd.DataFrame(),
                        cache_dir=str(tmp_path)).set_index("DUID")
    assert out.loc["IN1", "ISP_CURTAILMENT_FY1"] == 0.05
    assert pd.isna(out.loc["OUT1", "ISP_CURTAILMENT_FY1"])
    assert pd.isna(out.loc["UNK1", "ISP_CURTAILMENT_FY1"])
