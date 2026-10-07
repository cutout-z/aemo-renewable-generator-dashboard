"""2026 ISP Appendix A3 REZ curtailment: parser, crosswalk, merge, history and validator."""

import math
import re
from pathlib import Path

import pandas as pd
import pytest

from src import config, isp_rez_appendix as ia, rez_history
from src.excel_output import _get_column_spec
from src.merge import build_summary, isp_a3_columns

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

# Shapes copied from the 2026 ISP A3 (pdftotext -layout): a normal table (N1), a distribution
# zone with no REZ id and "-" years (Dubbo), "Accelerated / values / Transition" wrapped over
# three lines (N9), footnote lines after the table (V6), a combined "V3,V4 – ... REZ" heading,
# and T4, which has no table and says why. Form feeds are page breaks.
A3 = """\
A3.3.2 New South Wales
N1 – North West New South Wales

Summary
VRE curtailment

                                          2029-30                              2039-40                                       2049-50
Scenario                       Transmission                        Transmission
                                                  Economic spill                     Economic spill     Transmission curtailment       Economic spill
                                curtailment                         curtailment

Slower Growth                      0%                 11%               0%                   10%                  6%                       34%

Step Change                        6%                 18%               1%                   36%                  5%                       52%

Accelerated Transition             2%                  8%               0%                   35%                  0%                       48%

© AEMO 2026 | 2026 ISP Appendix A3. Renewable Energy Zones                                       20
\x0cDubbo distribution

Summary
VRE curtailment

                                        2029-30                                 2039-40                                        2049-50
Slower Growth                      -                  -                  -                    -                       -                        -
Step Change                        -                  -                 0%                   18%                     1%                      10%
Accelerated Transition             -                  -                 0%                   23%                     0%                      32%

N9 – Hunter-Central Coast

Summary
VRE curtailment
                                     2029-30                                 2039-40                                      2049-50
Slower Growth                0%                  29%                 2%               14%                  1%                            11%
Step Change                  13%                 27%                 0%               19%                  0%                            16%
Accelerated
                             6%                  10%                 0%               22%                  0%                            28%
Transition

A3.3.4 Tasmania
T4 – North Tasmania Coast

Summary
Transmission access expansion forecast and VRE curtailment
There are no existing, committed or anticipated VRE projects for this REZ, and the modelling outcomes, for all scenarios, did not project any additional VRE
for this REZ. Therefore, no VRE curtailment or transmission expansion occurs in this REZ.

A3.3.5 Victoria
V3,V4 – Western Victoria REZ

Summary
VRE curtailment
                                          2029-30                              2039-40                                       2049-50
Slower Growth                      0%                 20%               0%                   15%                  0%                       10%
Step Change                        0%                 16%               0%                   13%                  0%                       11%
Accelerated Transition             0%                 12%               0%                   12%                  0%                       12%

V6 – Gippsland Onshore

Summary
VRE curtailment
                                            2029-30                               2039-40                                       2049-50
Slower Growth                       0%                 12%                0%                   27%                    0%                       18%
Step Change                         0%                  7%                0%                   27%                    0%                       24%
Accelerated Transition              0%                  3%                0%                   23%                    0%                       21%
42
     For more information on the Latrobe Valley transmission modified parallel operating mode, see the 2025 Victorian Annual Planning Report.
"""


@pytest.fixture
def table():
    return ia.parse_a3_text(A3, "2026 ISP")


def _zone(t, rez_id, scenario="Step Change"):
    return t[(t["REZ_ID"] == rez_id) & (t["SCENARIO"] == scenario)].iloc[0]


def test_every_zone_and_scenario_is_read(table):
    zones = table.drop_duplicates(["REZ_ID", "REZ_NAME"])
    assert list(zip(zones["REZ_ID"], zones["REZ_NAME"], zones["STATE"])) == [
        ("N1", "North West New South Wales", "NSW"), ("", "Dubbo distribution", "NSW"),
        ("N9", "Hunter-Central Coast", "NSW"), ("T4", "North Tasmania Coast", "TAS"),
        ("V3,V4", "Western Victoria", "VIC"), ("V6", "Gippsland Onshore", "VIC")]
    assert len(table) == 6 * 3 and set(table["ISP_EDITION"]) == {"2026 ISP"}
    assert set(table["Y1_LABEL"]) == {"2029-30"} and set(table["Y3_LABEL"]) == {"2049-50"}


def test_values_by_scenario(table):
    n1 = _zone(table, "N1")
    assert [n1[c] for c in ia.VALUE_COLS] == pytest.approx([0.06, 0.01, 0.05, 0.18, 0.36, 0.52])
    assert _zone(table, "N1", "Slower Growth")["SPILL_Y3"] == pytest.approx(0.34)
    # wrapped "Accelerated / Transition" label, and a footnote after the table
    assert _zone(table, "N9", "Accelerated Transition")["SPILL_Y3"] == pytest.approx(0.28)
    assert _zone(table, "V6", "Accelerated Transition")["SPILL_Y1"] == pytest.approx(0.03)


def test_a_dash_is_empty_not_zero(table):
    dubbo = _zone(table, "")
    assert math.isnan(dubbo["TRANSMISSION_Y1"]) and math.isnan(dubbo["SPILL_Y1"])
    assert dubbo["TRANSMISSION_Y2"] == 0.0 and dubbo["SPILL_Y2"] == pytest.approx(0.18)


def test_a_zone_without_a_table_needs_a_reason(table):
    t4 = table[table["REZ_ID"] == "T4"]
    assert (~t4["HAS_TABLE"]).all() and t4[ia.VALUE_COLS].isna().all().all()
    silent = A3.replace("Therefore, no VRE curtailment or transmission expansion occurs in this REZ.", "")
    with pytest.raises(ia.ParseError, match="T4 North Tasmania Coast: no 'VRE curtailment' table"):
        ia.parse_a3_text(silent, "2026 ISP")


@pytest.mark.parametrize("old, new, message", [
    ("Step Change                        6%", "Step Change                        6", "unreadable table row"),
    ("Accelerated Transition             2%                  8%               0%                   35%                  0%                       48%\n", "",
     "no row for Accelerated Transition"),
    ("                                          2029-30                              2039-40                                       2049-50\nScenario", "Scenario",
     "no 'yyyy-yy' year header"),
])
def test_an_unreadable_table_fails_loudly(old, new, message):
    assert A3.count(old) >= 1
    with pytest.raises(ia.ParseError, match=message):
        ia.parse_a3_text(A3.replace(old, new, 1), "2026 ISP")


# ── Crosswalk and merge ──────────────────────────────────────────────────

def _crosswalk(rows):
    return pd.DataFrame([dict(zip(ia.CROSSWALK_COLUMNS, ["2026 ISP", *r[:2], "2025", *r[2:], ""]))
                         for r in rows])


CROSSWALK = _crosswalk([
    ("N1", "North West New South Wales", "N1", "Northwest New South Wales", "NSW", "renamed"),
    ("", "Dubbo distribution", "", "", "NSW", "no one-to-one match"),
    ("N9", "Hunter-Central Coast", "N9", "Hunter-Central Coast", "NSW", "exact"),
    ("T4", "North Tasmania Coast", "T4", "North Tasmania Coast", "TAS", "exact"),
    ("V3,V4", "Western Victoria", "V3", "Western Victoria", "VIC", "no one-to-one match"),
    ("V6", "Gippsland Onshore", "V5", "Gippsland", "VIC", "renamed"),
])


def _eli(names):
    return pd.DataFrame({"STATE": ["NSW"] * len(names), "REZ_NAME": names,
                         "ELI_EDITION": 2025, "ISP_EDITION": "2024 ISP"})


def test_only_one_to_one_pairs_are_joined():
    m = ia.page_mapping(CROSSWALK, "2026 ISP", 2025)
    assert m["northwest new south wales"] == ("N1", "North West New South Wales", "renamed")
    assert m["western victoria"] == ("", "", "no one-to-one match")
    # a page REZ listed twice (split) is never joined, even if one row says exact
    two = pd.concat([CROSSWALK, _crosswalk([("V2", "Central Highlands", "V5", "Gippsland", "VIC", "exact")])])
    assert ia.page_mapping(two, "2026 ISP", 2025)["gippsland"] == ("", "", "no one-to-one match")


def test_crosswalk_check_names_every_gap(table):
    eli = _eli(["Northwest New South Wales", "Hunter-Central Coast", "North Tasmania Coast",
                "Western Victoria", "Gippsland"])
    assert ia.check_crosswalk(CROSSWALK, table, eli) == []
    assert ia.not_in_a3(CROSSWALK, table, eli) == [
        "North Tasmania Coast: T4 North Tasmania Coast has no VRE curtailment table in A3",
        "Western Victoria: no one-to-one match"]
    problems = ia.check_crosswalk(CROSSWALK.iloc[1:], table, _eli(["Tumut"]))
    assert "2026 ISP zone N1 North West New South Wales is not in the crosswalk" in problems
    assert "ELI 2025 REZ Tumut is not in the crosswalk for the 2026 ISP" in problems
    bad = CROSSWALK.assign(match=CROSSWALK["match"].replace("exact", "close enough"))
    assert any("match types" in p for p in ia.check_crosswalk(bad, table, None))


def _gens():
    return pd.DataFrame({
        "DUID": ["NW1", "WV1", "OUT1", "UNK1"], "PROJECT_NAME": list("abcd"),
        "REZ": ["Y", "Y", "N", ""], "REZ_NAME": ["Northwest New South Wales", "Western Victoria", "Non-REZ", ""],
        "REZ_SOURCE": ["eli", "eli", "eli", ""], "STATE": ["NSW", "VIC", "NSW", "NSW"],
        "REGIONID": ["NSW1", "VIC1", "NSW1", "NSW1"], "NAMEPLATE_MW": [1.0] * 4, "FUEL_TYPE": ["Solar"] * 4,
    })


def test_summary_joins_through_the_crosswalk_and_leaves_redrawn_rezs_blank(table, tmp_path):
    rez = _eli(["Northwest New South Wales", "Western Victoria"]).assign(CURTAILMENT_FY1=[0.0, 0.0])
    out = build_summary(_gens(), pd.DataFrame(), pd.DataFrame(), rez, pd.DataFrame(),
                        cache_dir=str(tmp_path), isp_a3=table, isp_crosswalk=CROSSWALK)
    assert list(out.columns[-len(isp_a3_columns()):]) == isp_a3_columns()   # appended last
    out = out.set_index("DUID")
    assert out.loc["NW1", "ISPA3_TRANSMISSION_Y1"] == pytest.approx(0.06)   # Step Change
    assert out.loc["NW1", "ISPA3_SPILL_Y3"] == pytest.approx(0.52)
    assert tuple(out.loc["NW1", ["ISPA3_REZ_ID", "ISPA3_MATCH"]]) == ("N1", "renamed")
    assert out.loc["WV1", "ISPA3_MATCH"] == "no one-to-one match"
    for duid in ("WV1", "OUT1", "UNK1"):
        assert out.loc[duid, [c for c in out.columns if c.startswith(("ISPA3_TRANS", "ISPA3_SPILL"))]].isna().all()
    # labels, scenario and edition on every row, for the page headers
    assert set(out["ISPA3_Y2_LABEL"]) == {"2039-40"} and set(out["ISPA3_EDITION"]) == {"2026 ISP"}
    assert set(out["ISPA3_SCENARIO"]) == {config.ISP_A3_SCENARIO}
    labels = dict(_get_column_spec(out.reset_index()))
    assert labels["ISPA3_SPILL_Y1"] == "2026 ISP Step Change Econ Spill 2029-30"


def test_main_refuses_when_the_crosswalk_misses_a_zone(tmp_path):
    src = tmp_path / "a3.txt"
    src.write_text(A3, encoding="utf-8")
    CROSSWALK.iloc[1:].to_csv(tmp_path / config.ISP_REZ_CROSSWALK_FILE, index=False)
    assert ia.main([str(src), "--cache-dir", str(tmp_path)]) == 1
    assert not (tmp_path / config.ISP_A3_FILE).exists()
    assert not (tmp_path / config.REZ_HISTORY_FILE).exists()
    CROSSWALK.to_csv(tmp_path / config.ISP_REZ_CROSSWALK_FILE, index=False)
    assert ia.main([str(src), "--cache-dir", str(tmp_path), "--retrieved-at", "2026-10-07"]) == 0
    assert len(pd.read_feather(tmp_path / config.ISP_A3_FILE)) == 18
    # 5 zones with a table x 3 scenarios x 2 measures x 3 years
    assert len(rez_history.load(tmp_path / config.REZ_HISTORY_FILE)) == 90


# ── History: append-only ─────────────────────────────────────────────────

def _eli_rows(value=0.1, eli_edition=2025, isp="2024 ISP"):
    fc = pd.DataFrame([{"STATE": "NSW", "REZ_NAME": "Northwest New South Wales",
                        **{f"{m}_FY{i}": value for m in ("CURTAILMENT", "OFFLOADING") for i in (1, 2, 3)},
                        **{f"{m}_FY{i}_LABEL": f"{24 + i}-{25 + i}" for m in ("CURTAILMENT", "OFFLOADING")
                           for i in (1, 2, 3)},
                        "ELI_EDITION": eli_edition, "ISP_EDITION": isp}])
    return rez_history.rows_from_eli(fc, CROSSWALK, "2026-10-06")


def test_appending_a_new_edition_never_rewrites_earlier_rows(table, tmp_path):
    path = tmp_path / "history.csv"
    assert rez_history.append(path, _eli_rows()) == 6
    before = path.read_bytes()
    a3 = rez_history.rows_from_isp_a3(table, CROSSWALK, config.ISP_A3_SOURCE, "2026-10-07")
    assert rez_history.append(path, a3) == 90
    assert rez_history.append(path, _eli_rows(0.2, 2026, "2026 ISP")) == 6   # the next ELI edition
    after = path.read_bytes()
    assert after.startswith(before)                     # earlier rows byte for byte
    assert rez_history.append(path, _eli_rows()) == 0    # same edition again: skipped
    with pytest.raises(rez_history.HistoryConflict, match="append-only"):
        rez_history.append(path, _eli_rows(0.3))          # same edition, other figures: refused
    assert path.read_bytes() == after
    held = rez_history.load(path)
    assert rez_history.editions(held) == {
        ("ELI 2025 regional appendices", "2025", "2024 ISP"),
        ("2026 ISP Appendix A3", "", "2026 ISP"),
        ("ELI 2026 regional appendices", "2026", "2026 ISP")}
    row = held[(held["rez_id"] == "N1") & (held["scenario"] == "Step Change")
               & (held["measure"] == "economic_spill") & (held["fy"] == "2049-50")].iloc[0]
    assert (row["value"], row["crosswalk_key"], row["crosswalk_match"]) == ("0.52", "Northwest New South Wales", "renamed")
    # "-" is an empty value, not 0
    dubbo = held[(held["rez_name"] == "Dubbo distribution") & (held["fy"] == "2029-30")]
    assert set(dubbo["value"]) == {""}


# ── The committed reference files ────────────────────────────────────────

def test_committed_files_agree():
    table = pd.read_feather(DATA / config.ISP_A3_FILE)
    eli = pd.read_feather(DATA / "rez_forecasts.feather")
    crosswalk = ia.load_crosswalk(DATA)
    assert ia.edition(table) == "2026 ISP"
    assert ia.check_crosswalk(crosswalk, table, eli) == []
    # hand-checked against the A3 PDF: N1 North West NSW, Step Change, 6/18 | 1/36 | 5/52
    n1 = _zone(table, "N1")
    assert [n1[c] for c in ia.VALUE_COLS] == pytest.approx([0.06, 0.01, 0.05, 0.18, 0.36, 0.52])
    assert table.drop_duplicates(["REZ_ID", "REZ_NAME"]).shape[0] == 46
    history = rez_history.load(DATA / config.REZ_HISTORY_FILE)
    assert rez_history.editions(history) == {("ELI 2025 regional appendices", "2025", "2024 ISP"),
                                             ("2026 ISP Appendix A3", "", "2026 ISP")}
    assert set(crosswalk["match"]) == set(ia.MATCH_TYPES)


def test_page_reads_the_a3_labels_from_the_data():
    page = (ROOT / "index.html").read_text(encoding="utf-8")
    assert "pick('ISPA3_EDITION')" in page and "pick('ISPA3_SCENARIO')" in page
    assert "ISPA3_Y${i}_LABEL" in page
    assert "'a3-t':" in page and "'a3-s':" in page and ".fam-a3-t" in page
    note = re.search(r'<p[^>]*id="ispA3Note"[^>]*>(.*?)</p>', page, re.S).group(1)
    assert "Appendix A3" in note and "N/A" in note
    # the 2024 ISP columns and their superseded note stay
    assert 'id="ispNote"' in page and "'isp-c'" in page


def test_readme_documents_definitions_crosswalk_and_history():
    readme = " ".join((ROOT / "README.md").read_text(encoding="utf-8").split())
    assert "network curtailment" in readme and "economic spill" in readme
    assert "isp_rez_crosswalk.csv" in readme and "no one-to-one match" in readme
    assert "python -m src.rez_history eli" in readme and "append-only" in readme


# ── Validator ────────────────────────────────────────────────────────────

def _cache(tmp_path, table, history=True):
    eli = _eli(["Northwest New South Wales", "Hunter-Central Coast", "North Tasmania Coast",
                "Western Victoria", "Gippsland"])
    table.to_feather(tmp_path / config.ISP_A3_FILE)
    eli.to_feather(tmp_path / "rez_forecasts.feather")
    CROSSWALK.to_csv(tmp_path / config.ISP_REZ_CROSSWALK_FILE, index=False)
    if history:
        rez_history.append(tmp_path / config.REZ_HISTORY_FILE,
                           rez_history.rows_from_isp_a3(table, CROSSWALK, config.ISP_A3_SOURCE, "2026-10-07"))
        rez_history.append(tmp_path / config.REZ_HISTORY_FILE, _eli_rows())
    rez = eli.assign(CURTAILMENT_FY1=0.0)
    return build_summary(_gens(), pd.DataFrame(), pd.DataFrame(), rez, pd.DataFrame(),
                         isp_a3=table, isp_crosswalk=CROSSWALK).assign(ELI_EDITION=2025)


def _check(df, cache):
    import validate_outputs as vo
    vo.errors.clear()
    vo.check_isp_a3(df, cache)
    found = list(vo.errors)
    vo.errors.clear()
    return found


def test_validator_passes_matching_files(table, tmp_path):
    assert _check(_cache(tmp_path, table), tmp_path) == []


def test_validator_fails_a_missing_history_edition(table, tmp_path):
    errors = _check(_cache(tmp_path, table, history=False), tmp_path)
    assert any("lacks the edition the page shows" in e and "2026 ISP Appendix A3" in e for e in errors)
    assert any("ELI 2025 regional appendices" in e for e in errors)


def test_validator_fails_an_edition_mismatch_or_an_unmapped_rez(table, tmp_path):
    df = _cache(tmp_path, table)
    assert any("ISPA3_EDITION" in e for e in _check(df.assign(ISPA3_EDITION="2028 ISP"), tmp_path))
    CROSSWALK.iloc[:-1].to_csv(tmp_path / config.ISP_REZ_CROSSWALK_FILE, index=False)
    errors = _check(df, tmp_path)
    assert any("V6 Gippsland Onshore is not in the crosswalk" in e for e in errors)
    assert any("Gippsland is not in the crosswalk" in e for e in errors)


def test_validator_fails_summary_columns_without_the_a3_file(table, tmp_path):
    df = _cache(tmp_path, table)
    (tmp_path / config.ISP_A3_FILE).unlink()
    assert any("ISPA3_* columns" in e for e in _check(df, tmp_path))
    assert _check(df.drop(columns=isp_a3_columns()), tmp_path) == []   # nothing built: a warning only
