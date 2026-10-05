"""Registration List columns map to the descriptor (TECHNOLOGY) and primary (fuel) columns."""

from src import download_generators as dg
from fixtures import DEFAULT_REG_ROWS, reg_row, registration_xlsx


def test_technology_is_the_descriptor_not_renewable(tmp_path):
    hybrid_battery = reg_row("HPR1", "Hornsdale Power Reserve", "SA1", "Battery Storage",
                             "Battery and Inverter", 150, primary_tech="Storage")
    hybrid_battery["Fuel Source - Descriptor"] = "Wind"
    path = tmp_path / "reg.xls"
    path.write_bytes(registration_xlsx(DEFAULT_REG_ROWS + [hybrid_battery]))

    df = dg._parse_registration_list(path).set_index("DUID")

    assert df.loc["ARWF1", "TECHNOLOGY"] == "Wind - Onshore"
    assert df.loc["WANDSF2", "TECHNOLOGY"] == "Photovoltaic Tracking Flat panel"
    assert "Renewable" not in set(df["TECHNOLOGY"])
    assert df.loc["ARWF1", "FUEL_TYPE"] == "Wind"
    assert df.loc["WANDSF2", "FUEL_TYPE"] == "Solar"
    # A battery whose descriptor names its co-located fuel is not a wind farm
    assert "HPR1" not in df.index
    assert "ER01" not in df.index


def test_map_columns_prefers_earlier_candidates_and_exact_headers():
    cols = ["Technology Type - Primary", "Technology Type - Descriptor", "Region", "Sub-region"]
    m = dg._map_columns(cols, {
        "TECHNOLOGY": ["technology type - descriptor", "technology type"],
        "REGIONID": ["region"],
    })
    assert m == {"Technology Type - Descriptor": "TECHNOLOGY", "Region": "REGIONID"}
