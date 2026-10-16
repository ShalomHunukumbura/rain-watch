# rain-watch

Is it unusually wet, or unusually dry, in your district? Every morning this pipeline takes
yesterday's rainfall for all 25 districts of Sri Lanka, compares the last 7 days with the
same week in every year since 2015, adds the 3-day forecast, publishes a page, and sends a
phone alert when a district turns unusually wet or heavy rain is forecast.

It runs on [Apache Airflow](https://airflow.apache.org/), with the data in an S3-compatible
data lake.

**Not an official warning.** Follow the [Department of Meteorology](https://www.meteo.gov.lk/)
and the [Disaster Management Centre](https://www.dmc.gov.lk/) for those.

## How it works

Two DAGs (an Airflow pipeline is called a DAG: tasks, and the order they run in):

```
rain_daily, every day at 07:00 Colombo time
  wait_for_data -> extract -> validate -> load -> only_latest -> forecast -> assess -> alert -> publish

rain_history, one run per month, used to backfill 2015 to now
  extract -> validate -> load
```

- **wait_for_data** is a sensor: it checks every 30 minutes until Open-Meteo has published
  yesterday for every district.
- **extract** saves Open-Meteo's answer untouched in the lake. The daily run re-reads the last
  8 days, because the source replaces its first estimate with final values a few days later.
- **validate** is the quality gate: every district and day present, no missing values,
  rain between 0 and 1,000 mm, rain hours between 0 and 24, and districts not all identical.
  If any check fails, the task fails and nothing after it runs.
- **load** writes one Parquet file per day, `clean/rainfall/date=YYYY-MM-DD/data.parquet`.
  Re-running a day replaces its file, so one past day can be re-run on its own.
- **only_latest** skips everything after it on old runs, so a backfill never sends alerts
  about last year.
- **assess** compares each district's last 7 days with the same week (give or take 7 days) in
  every earlier year, with DuckDB reading the lake directly.
- **alert** posts new alerts to an [ntfy.sh](https://ntfy.sh) topic, only ones that weren't
  raised the day before.
- **publish** writes `site/data/latest.json` and `history.json`.

The lake is [SeaweedFS](https://github.com/seaweedfs/seaweedfs) in Docker. The code speaks plain
S3, so MinIO, Cloudflare R2 or AWS S3 work the same way.

## Run it

You need Docker, and [uv](https://docs.astral.sh/uv/) for the tests. Airflow, Postgres and
SeaweedFS together use about 1 GB of memory.

```bash
cp .env.example .env              # then set AIRFLOW_UID to the output of: id -u
docker compose up -d --build      # Airflow UI at http://localhost:8080 after a minute

# Fill in history: about 140 monthly runs, about 15 minutes.
docker compose exec airflow airflow dags unpause rain_history
docker compose exec airflow airflow backfill create --dag-id rain_history \
    --from-date 2015-01-01 --to-date "$(date +%F)"

# Run the daily pipeline once now, then switch it on.
docker compose exec airflow airflow dags test rain_daily "$(date +%FT07:00:00+05:30)"
docker compose exec airflow airflow dags unpause rain_daily

uv run pytest                     # the pipeline steps, with made-up weather
```

Open `site/index.html` through a web server to see the page:
`python3 -m http.server --directory site 8000`.

## Publishing

Airflow runs on your machine and writes `site/data/`. `scripts/publish.sh` commits and pushes
it, and the `pages` workflow publishes `site/` to GitHub Pages. Run the script on a timer
after the morning run (see `explain.md`, or the systemd timer used for `lkr-rates`).

## Data

Rainfall from [Open-Meteo.com](https://open-meteo.com/), licensed
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/): ERA5 reanalysis from the Copernicus
Climate Change Service for past days, national weather service models for the forecast.
Open-Meteo's free API is for non-commercial use.

Each district is represented by one point, its main town (`src/rain_watch/districts.csv`).
Rain varies a lot within a district, and model rainfall is not a rain gauge.

## Licence

The code is MIT licensed. The data is Open-Meteo's, under CC BY 4.0.
