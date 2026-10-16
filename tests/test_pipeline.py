"""Runs the pipeline steps end to end against a folder on disk, with made-up weather.

Eleven years of history around mid-October: about 5 mm a day, a little wetter or drier
from year to year. In the week to 15 October 2026, Colombo gets 40 mm a day, Jaffna gets
none, and everywhere else gets a middling week.
"""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import pytest
from conftest import fake_response

from rain_watch import openmeteo, pipeline
from rain_watch.checks import QualityGateFailed
from rain_watch.districts import load_districts
from rain_watch.lake import LocalLake

DAY = date(2026, 10, 15)


def history_rain(name: str, d: date) -> float:
    # Year offsets cycle through 0, 0.5, 1, 1.5 and 2 mm: no trend.
    return 5.0 + (d.year * 3 % 5) * 0.5 + (len(name) % 3)


def this_week(name: str, d: date) -> float:
    if name == "Colombo":
        return 40.0
    if name == "Jaffna":
        return 0.0
    return 5.0 + 1.0 + (len(name) % 3)  # the middle of the historical range


@pytest.fixture(scope="session")
def history_lake(tmp_path_factory) -> Path:
    """Eleven years of history, built once and copied for each test."""
    districts = load_districts()
    root = tmp_path_factory.mktemp("history") / "lake"
    lake = LocalLake(root)
    for year in range(2015, 2026):
        start, end = date(year, 9, 25), date(year, 10, 31)
        key = f"raw/observed/{start}_{end}/test.json"
        lake.put(key, fake_response(districts, start, end, history_rain))
        pipeline.load(lake, key, districts)
    return root


@pytest.fixture
def lake(history_lake, tmp_path, monkeypatch):
    shutil.copytree(history_lake, tmp_path / "lake")
    lake = LocalLake(tmp_path / "lake")

    def fetch_observed(ds, start, end):
        return fake_response(ds, start, end, this_week)

    def fetch_forecast(ds, days=3):
        rain = lambda name, d: 120.0 if name == "Ratnapura" else 2.0  # noqa: E731
        return fake_response(ds, date(2026, 10, 16), date(2026, 10, 18), rain)

    monkeypatch.setattr(openmeteo, "fetch_observed", fetch_observed)
    monkeypatch.setattr(openmeteo, "fetch_forecast", fetch_forecast)
    return lake


def run_day(lake, districts):
    start, end = pipeline.daily_window(DAY)
    raw = pipeline.extract_observed(lake, districts, start, end)
    pipeline.validate(lake, raw, districts, start, end)
    pipeline.load(lake, raw, districts)
    forecast = pipeline.extract_forecast(lake, districts, DAY)
    return pipeline.assess(lake, districts, DAY, forecast)


def by_district(assessment):
    return {d["district"]: d for d in assessment["districts"]}


def test_one_partition_per_day(lake, districts):
    run_day(lake, districts)
    assert lake.exists("clean/rainfall/date=2026-10-15/data.parquet")
    assert lake.exists("clean/rainfall/date=2026-10-08/data.parquet")  # the 7-day refresh
    assert not lake.exists("clean/rainfall/date=2026-10-07/data.parquet")


def test_levels(lake, districts):
    found = by_district(run_day(lake, districts))
    assert found["Colombo"]["total_7d"] == 280.0
    assert found["Colombo"]["level"] == "Much wetter than normal"
    assert found["Jaffna"]["level"] == "Much drier than normal"
    assert found["Kandy"]["level"] == "Normal"
    assert found["Colombo"]["baseline_weeks"] == 11 * 15
    assert found["Colombo"]["last_7_days"] == [40.0] * 7


def test_forecast_level(lake, districts):
    found = by_district(run_day(lake, districts))
    assert found["Ratnapura"]["forecast_level"] == "Heavy rain likely"
    assert found["Ratnapura"]["forecast_max_mm"] == 120.0
    assert found["Kandy"]["forecast_level"] == "No heavy rain"


def test_rerunning_a_day_gives_the_same_answer(lake, districts):
    first = run_day(lake, districts)
    second = run_day(lake, districts)
    first.pop("generated_at"), second.pop("generated_at")
    assert first == second


def test_alerts_are_sent_once(lake, districts):
    assessment = run_day(lake, districts)
    assert pipeline.new_alerts(None, assessment) == [
        ("Colombo", "Much wetter than normal"),
        ("Ratnapura", "Heavy rain likely"),
    ]
    # The next morning nothing has changed, so nothing new to say.
    assert pipeline.new_alerts(assessment, assessment) == []


def test_publish_writes_the_page_files(lake, districts, tmp_path):
    assessment = run_day(lake, districts)
    pipeline.publish(lake, assessment, tmp_path / "site" / "data")
    latest = json.loads((tmp_path / "site" / "data" / "latest.json").read_text())
    assert latest["date"] == "2026-10-15"
    assert len(latest["districts"]) == 25
    history = json.loads(lake.get("public/history.json"))
    assert history["dates"][-1] == "2026-10-15"
    assert history["districts"]["Colombo"][-1] == 40.0


def test_bad_data_stops_at_the_gate(lake, districts, monkeypatch):
    monkeypatch.setattr(
        openmeteo, "fetch_observed", lambda ds, s, e: fake_response(ds, s, e, lambda n, d: -3.0)
    )
    start, end = pipeline.daily_window(DAY)
    raw = pipeline.extract_observed(lake, districts, start, end)
    with pytest.raises(QualityGateFailed, match="rainfall is between"):
        pipeline.validate(lake, raw, districts, start, end)
