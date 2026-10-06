"""Page and README say what each curtailment family measures (M4)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PAGE = (ROOT / "index.html").read_text(encoding="utf-8")
README = (ROOT / "README.md").read_text(encoding="utf-8")


def test_page_states_each_family_and_that_they_differ():
    assert "not comparable" in PAGE
    assert "economic curtailment" in PAGE          # actual includes it
    assert "300 MW" in PAGE                        # ELI is a hypothetical new project
    assert "const GROUP_NOTES" in PAGE and 'title="${GROUP_NOTES[g]}"' in PAGE
    for family in ("actual:", "eli:", "'isp-c':", "'isp-o':"):
        assert family in PAGE.split("const GROUP_NOTES", 1)[1].split("};", 1)[0]


def test_readme_no_longer_calls_eli_network_constraints_only():
    assert "curtailed due to network constraints" not in README
    assert "100 MW" not in README and "300 MW" in README
    assert "economic curtailment" in README and "not comparable" in README


def test_readme_actual_formula_is_the_energy_ratio():
    assert "Σ(monthly_curtailment × monthly_generation)" not in README
    assert "1 − Σ(eligible actual MWh) / Σ(potential MWh)" in README
