"""REZ membership and REZ forecasts from the ELI regional appendices (pdftotext -layout)."""

import math

import pandas as pd
import pytest

from src import download_generators as dg
from src import eli_appendix as ea
from src.download_rez import fetch_rez_forecasts

# Shapes copied from the 2025 appendices: the contents page, a normal forecast
# table, a "-" year (no VRE projected), the QLD layout that wraps "Step Change"
# over two lines, an all-caps DUID without a digit, and a unit listed under a
# REZ and again under Non-REZ (the hosting-capacity reference set).
APPENDIX = """\
A3.6    N2 – New England                                                                          17
A3.17 Non-REZ                                                                                     55

A3.6            N2 – New England
    VRE semi-scheduled curtailment – calendar year 2024
    DUID                      Generator name                Capacity (MW)        curtailment (%)
    NEWENSF2              New England Solar Farm                  200                     2.6
    SAPHWF1                 Sapphire Wind Farm                    270                     0.0
    VRE curtailment – ISP forecast
                                    2025-2026                                2026-2027                           2027-2028
    Scenario                                 Economic                                Economic                              Economic
                     Curtailment (%)                          Curtailment (%)                        Curtailment (%)
    Step Change             10                     16               15                     26              24                  34

A3.13 N9 – Hunter-Central Coast
    VRE curtailment – ISP forecast
                                2025-2026                                2026-2027                          2027-2028
    Step Change             -                    -                     3                5                 3                 6

A4.9 Q5 – Barcaldine
    LRSF1                      Longreach Solar Farm                    15                  1.2
    CATHROCK                    Cathedral Rocks                         66
    VRE curtailment – ISP forecast
                                  2025-2026                                   2026-2027                            2027-2028
 Step
                        0                          1                      0               13                  0                12
 Change

A3.17 Non-REZ
    Constraint ID                Binding hours      Marginal value ($)
    N>>NIL_966/1                     125.0             425,861.4
    NEWENSF2                   New England Solar Farm                 300                 300
    COLEASF1                    Coleambally Solar Farm                 0                  300
"""


@pytest.fixture
def parsed():
    return ea.parse_appendix_text(APPENDIX, "NSW")


def test_membership_reads_each_section_and_ignores_the_contents_page(parsed):
    membership, _ = parsed
    got = dict(zip(membership["DUID"], membership["REZ_NAME"]))
    assert got == {
        "NEWENSF2": "New England",   # listed under Non-REZ too: the REZ wins
        "SAPHWF1": "New England",
        "LRSF1": "Barcaldine",
        "CATHROCK": "Barcaldine",    # all-caps DUID with no digit
        "COLEASF1": "Non-REZ",
    }
    assert set(membership.loc[membership["REZ_NAME"] == "Non-REZ", "REZ_ID"]) == {""}
    assert set(membership.loc[membership["REZ_NAME"] == "New England", "REZ_ID"]) == {"N2"}


def test_forecasts_are_fractions_with_fy_labels(parsed):
    _, forecasts = parsed
    ne = forecasts.set_index("REZ_NAME").loc["New England"]
    assert [ne[f"CURTAILMENT_FY{i}"] for i in (1, 2, 3)] == [0.10, 0.15, 0.24]
    assert [ne[f"OFFLOADING_FY{i}"] for i in (1, 2, 3)] == [0.16, 0.26, 0.34]
    assert [ne[f"CURTAILMENT_FY{i}_LABEL"] for i in (1, 2, 3)] == ["25-26", "26-27", "27-28"]
    assert ne["OFFLOADING_AVG"] == pytest.approx((0.16 + 0.26 + 0.34) / 3)


def test_a_dash_year_is_empty_not_zero(parsed):
    _, forecasts = parsed
    hcc = forecasts.set_index("REZ_NAME").loc["Hunter-Central Coast"]
    assert math.isnan(hcc["CURTAILMENT_FY1"]) and math.isnan(hcc["OFFLOADING_FY1"])
    assert hcc["CURTAILMENT_FY3"] == 0.03
    assert hcc["CURTAILMENT_AVG"] == pytest.approx(0.03)  # over the two years with a value


def test_wrapped_step_change_row_is_read(parsed):
    _, forecasts = parsed
    b = forecasts.set_index("REZ_NAME").loc["Barcaldine"]
    assert [b[f"OFFLOADING_FY{i}"] for i in (1, 2, 3)] == [0.01, 0.13, 0.12]


def test_non_rez_has_no_forecast_row(parsed):
    _, forecasts = parsed
    assert "Non-REZ" not in set(forecasts["REZ_NAME"])


def test_fetch_rez_forecasts_reads_the_extracted_file(tmp_path):
    assert fetch_rez_forecasts(str(tmp_path)).empty
    _, forecasts = ea.parse_appendix_text(APPENDIX, "NSW")
    forecasts.to_feather(tmp_path / "rez_forecasts.feather")
    assert len(fetch_rez_forecasts(str(tmp_path))) == 3


def _gens(*duids):
    return pd.DataFrame({"DUID": list(duids), "FUEL_TYPE": ["Wind"] * len(duids)})


def test_assign_rez_prefers_the_appendix_then_station_siblings_then_seed():
    membership, _ = ea.parse_appendix_text(APPENDIX, "NSW")
    seed = pd.DataFrame({"DUID": ["SAPHWF1", "OLD1"], "REZ": ["N", "N"],
                         "REZ_NAME": ["Non-REZ", "Non-REZ"]})
    stations = {"SAPHWF1": "SAPHWF", "SAPHWF2": "SAPHWF", "LRSF1": "LRSF", "OLD1": "OLD"}
    out = dg.assign_rez(_gens("SAPHWF1", "SAPHWF2", "COLEASF1", "OLD1", "NEW1"),
                        seed=seed, membership=membership, stations=stations).set_index("DUID")
    rez = lambda d: tuple(out.loc[d, ["REZ", "REZ_NAME", "REZ_SOURCE"]])
    assert rez("SAPHWF1") == ("Y", "New England", "eli")          # appendix beats the seed
    assert rez("SAPHWF2") == ("Y", "New England", "eli-station")  # same station as SAPHWF1
    assert rez("COLEASF1") == ("N", "Non-REZ", "eli")
    assert rez("OLD1") == ("N", "Non-REZ", "seed")
    assert rez("NEW1") == ("", "", "")                            # no source: unknown


def test_station_split_across_sections_is_not_inherited():
    membership = pd.DataFrame({"STATE": ["VIC"] * 2, "REZ_ID": ["V4", ""],
                               "REZ_NAME": ["South West Victoria", "Non-REZ"],
                               "DUID": ["A1", "A2"], "GENERATOR": ["A", "A"]})
    out = dg.assign_rez(_gens("A3"), membership=membership,
                        stations={"A1": "ST", "A2": "ST", "A3": "ST"}).iloc[0]
    assert (out["REZ"], out["REZ_SOURCE"]) == ("", "")
