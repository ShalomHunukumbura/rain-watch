"""rain_daily: every morning, yesterday's rainfall for every district, checked and compared.

    wait_for_data -> extract -> validate -> load -> only_latest -> forecast -> assess
                                                                                |
                                                                       alert -> publish

The quality gate is validate: if it fails, nothing after it runs, so bad data never
reaches the clean partitions or the page. only_latest skips the forecast, alerts and
publishing on old runs (a backfill must not send alerts about last March).
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.sdk import PokeReturnValue, dag, get_current_context, task

from rain_watch import openmeteo, pipeline
from rain_watch.districts import load_districts
from rain_watch.lake import lake_from_env

COLOMBO = pendulum.timezone("Asia/Colombo")


def target_day() -> date:
    """The day this run is about: the day before the run was scheduled, in Colombo."""
    logical = get_current_context()["logical_date"]
    return logical.in_timezone(COLOMBO).date() - timedelta(days=1)


@dag(
    schedule="0 7 * * *",  # 07:00 in Colombo
    start_date=pendulum.datetime(2026, 10, 1, tz=COLOMBO),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=15),
        "retry_exponential_backoff": True,
    },
    tags=["rain-watch"],
    doc_md=__doc__,
)
def rain_daily():
    # A sensor waits for something outside Airflow to be true. Here: Open-Meteo has
    # published yesterday for every district. "reschedule" frees the worker slot between
    # checks instead of sleeping in it.
    @task.sensor(poke_interval=30 * 60, timeout=12 * 60 * 60, mode="reschedule", pool="open_meteo")
    def wait_for_data() -> PokeReturnValue:
        day = target_day()
        raw = openmeteo.fetch_observed(load_districts(), day, day)
        return PokeReturnValue(is_done=openmeteo.has_all_values(raw, day))

    @task(pool="open_meteo")
    def extract() -> str:
        start, end = pipeline.daily_window(target_day())
        return pipeline.extract_observed(lake_from_env(), load_districts(), start, end)

    @task
    def validate(raw_key: str) -> str:
        start, end = pipeline.daily_window(target_day())
        return pipeline.validate(lake_from_env(), raw_key, load_districts(), start, end)

    @task
    def load(raw_key: str) -> list[str]:
        return pipeline.load(lake_from_env(), raw_key, load_districts())

    only_latest = LatestOnlyOperator(task_id="only_latest")

    @task(pool="open_meteo")
    def forecast() -> str:
        return pipeline.extract_forecast(lake_from_env(), load_districts(), target_day())

    @task
    def assess(forecast_key: str) -> dict:
        return pipeline.assess(lake_from_env(), load_districts(), target_day(), forecast_key)

    @task
    def alert(assessment: dict) -> str:
        previous = pipeline.previous_assessment(lake_from_env())
        return pipeline.send_alerts(pipeline.new_alerts(previous, assessment), assessment["date"])

    @task
    def publish(assessment: dict) -> list[str]:
        site = os.environ.get("RAIN_WATCH_SITE_DIR")
        return pipeline.publish(lake_from_env(), assessment, Path(site) if site else None)

    raw = extract()
    wait_for_data() >> raw
    checked = validate(raw)
    loaded = load(raw)
    checked >> loaded >> only_latest
    forecast_key = forecast()
    only_latest >> forecast_key
    assessment = assess(forecast_key)
    # Alert before publishing: alerting compares with the previous published day.
    alert(assessment) >> publish(assessment)


rain_daily()
