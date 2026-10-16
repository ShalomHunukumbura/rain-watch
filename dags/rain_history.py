"""rain_history: one month of rainfall per run, for filling in the past.

    extract -> validate -> load

Fill in history with Airflow's backfill, which creates one run per month. The scheduler
only runs a DAG that is switched on, backfills included, so unpause it first:

    airflow dags unpause rain_history
    airflow backfill create --dag-id rain_history --from-date 2015-01-01 --to-date 2026-10-16

One request covers a whole month for all 25 districts. Open-Meteo counts that as about 55
of its 10,000 free calls a day and 5,000 an hour, so about 130 months fit in a day. If it
answers "too many requests", the task fails and Airflow retries it later, with a longer
wait each time.

Scheduled runs, on the 1st of each month, cover that month up to yesterday, which on the
1st is nothing, so they skip. The daily DAG keeps recent days; this one is for the past.
"""

from __future__ import annotations

from datetime import date, timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, get_current_context, task

from rain_watch import pipeline
from rain_watch.districts import load_districts
from rain_watch.lake import lake_from_env

COLOMBO = pendulum.timezone("Asia/Colombo")


def month_window() -> tuple[date, date]:
    """The month of this run, up to yesterday at the latest."""
    first = get_current_context()["logical_date"].in_timezone(COLOMBO).date().replace(day=1)
    last = (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    yesterday = pendulum.now(COLOMBO).date() - timedelta(days=1)
    end = min(last, yesterday)
    if end < first:
        raise AirflowSkipException(f"nothing to fetch yet for {first:%B %Y}")
    return first, end


@dag(
    schedule="0 3 1 * *",
    start_date=pendulum.datetime(2015, 1, 1, tz=COLOMBO),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=2,
    default_args={
        "retries": 6,
        "retry_delay": timedelta(minutes=20),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=6),
    },
    tags=["rain-watch"],
    doc_md=__doc__,
)
def rain_history():
    @task(pool="open_meteo")
    def extract() -> str:
        start, end = month_window()
        return pipeline.extract_observed(lake_from_env(), load_districts(), start, end)

    @task
    def validate(raw_key: str) -> str:
        start, end = month_window()
        return pipeline.validate(lake_from_env(), raw_key, load_districts(), start, end)

    @task
    def load(raw_key: str) -> list[str]:
        return pipeline.load(lake_from_env(), raw_key, load_districts())

    raw = extract()
    validate(raw) >> load(raw)


rain_history()
