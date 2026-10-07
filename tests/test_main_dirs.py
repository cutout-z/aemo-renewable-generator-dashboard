"""`--cache-dir` / `--output-dir` keep a run away from data/ and outputs/."""

import pandas as pd

from src import main as main_mod


def _generators():
    return pd.DataFrame({
        "DUID": ["AAASF1", "BBBWF1"],
        "PROJECT_NAME": ["Aaa Solar Farm", "Bbb Wind Farm"],
        "REZ": ["", ""],
        "REZ_NAME": ["", ""],
        "STATE": ["NSW", "VIC"],
        "REGIONID": ["NSW1", "VIC1"],
        "NAMEPLATE_MW": [100.0, 200.0],
        "FUEL_TYPE": ["Solar", "Wind"],
    })


def test_run_writes_only_to_the_given_directories(tmp_path, monkeypatch):
    cache = tmp_path / "cache"
    out = tmp_path / "out"
    cache.mkdir()
    seen = {}

    def fake_fetch_generators(cache_dir, *args, **kwargs):
        seen["generators"] = cache_dir
        return _generators()

    monkeypatch.setattr(main_mod, "fetch_generators", fake_fetch_generators)
    monkeypatch.setattr(main_mod, "fetch_mlf_data", lambda cache_dir, **k: pd.DataFrame())
    monkeypatch.setattr(main_mod, "fetch_eli_curtailment", lambda cache_dir: pd.DataFrame())
    monkeypatch.setattr(main_mod, "fetch_rez_forecasts", lambda cache_dir: pd.DataFrame())
    monkeypatch.setattr(main_mod, "fetch_curtailment_by_fy", lambda duids: pd.DataFrame())

    repo_summary = main_mod.PROJECT_ROOT / "outputs" / "summary.csv"
    before = repo_summary.stat().st_mtime if repo_summary.exists() else None

    main_mod.run(full_refresh=True, cache_dir=str(cache), output_dir=str(out))

    assert seen["generators"] == str(cache)
    assert (out / "summary.csv").exists()
    assert (out / "NSW_curtailment.xlsx").exists()
    assert (out / "source_status.json").exists()   # the footer's trimmed status (S3-1)
    assert (cache / "generators.feather").exists()
    after = repo_summary.stat().st_mtime if repo_summary.exists() else None
    assert before == after
