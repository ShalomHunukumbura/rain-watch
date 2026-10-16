"""Each pipeline step as a plain function. The Airflow tasks are thin wrappers around these.

Keys in the lake:

  raw/observed/<start>_<end>/<fetched>.json   Open-Meteo's answer, untouched
  raw/forecast/<day>/<fetched>.json
  clean/rainfall/date=<day>/data.parquet      one partition per day, all 25 districts
  public/latest.json                          what the page and the alerts read
"""

from __future__ import annotations

import json
import os
import tempfile
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from . import checks, openmeteo
from .districts import District
from .lake import Lake
from .parse import parse

# The daily run re-reads this many days before the target day. Open-Meteo's archive
# starts with a quick estimate and replaces it with the final reanalysis about five days
# later, so re-reading a week picks up those corrections.
REFRESH_DAYS = 7

# Observed levels: how today's 7-day total ranks against the same week in earlier years.
MUCH_WETTER = 0.95
WETTER = 0.80
MUCH_DRIER = 0.05
MIN_BASELINE = 20  # fewer comparable weeks than this and we don't judge
# Forecast levels, in the Department of Meteorology's wording.
HEAVY_MM = 100.0
FAIRLY_HEAVY_MM = 75.0


def _now() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def extract_observed(lake: Lake, districts: list[District], start: date, end: date) -> str:
    raw = openmeteo.fetch_observed(districts, start, end)
    key = f"raw/observed/{start}_{end}/{_now()}.json"
    lake.put(key, raw)
    return key


def extract_forecast(lake: Lake, districts: list[District], day: date) -> str:
    raw = openmeteo.fetch_forecast(districts)
    key = f"raw/forecast/{day}/{_now()}.json"
    lake.put(key, raw)
    return key


def validate(lake: Lake, raw_key: str, districts: list[District], start: date, end: date) -> str:
    """The quality gate. Raises QualityGateFailed if any check is an error."""
    problems = checks.check(parse(lake.get(raw_key), districts), districts, start, end)
    text = checks.report(problems)
    if checks.errors(problems):
        raise checks.QualityGateFailed(f"{raw_key}\n{text}")
    return text


def load(lake: Lake, raw_key: str, districts: list[District]) -> list[str]:
    """Write one clean partition per day. Rewriting a day replaces it, so re-runs are safe."""
    import duckdb

    readings = parse(lake.get(raw_key), districts)
    fetched_at = raw_key.rsplit("/", 1)[1].removesuffix(".json")
    province = {d.name: d.province for d in districts}
    keys = []
    with tempfile.TemporaryDirectory() as tmp:
        # Rows go in through a JSON-lines file (inserting one by one is slow), and one
        # COPY with PARTITION_BY writes a folder per day: date=2026-10-15/data_0.parquet.
        rows = Path(tmp) / "rows.jsonl"
        with rows.open("w") as f:
            for r in readings:
                f.write(
                    json.dumps(
                        {
                            "day": r.day.isoformat(),
                            "district": r.district,
                            "province": province[r.district],
                            "precipitation_mm": r.precipitation_mm,
                            "precipitation_hours": r.precipitation_hours,
                            "fetched_at": fetched_at,
                        }
                    )
                    + "\n"
                )
        out = Path(tmp) / "out"
        with duckdb.connect() as conn:
            conn.execute(
                f"""
                COPY (
                    SELECT day, day AS date, district, province,
                           precipitation_mm::DECIMAL(6,1) AS precipitation_mm,
                           precipitation_hours::DECIMAL(4,1) AS precipitation_hours,
                           fetched_at
                    FROM read_json('{rows}', format = 'newline_delimited', columns = {{
                        'day': 'DATE', 'district': 'VARCHAR', 'province': 'VARCHAR',
                        'precipitation_mm': 'DOUBLE', 'precipitation_hours': 'DOUBLE',
                        'fetched_at': 'VARCHAR'}})
                    ORDER BY day, district
                ) TO '{out}' (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (date))
                """
            )
        for part in sorted(out.glob("date=*/*.parquet")):
            key = f"clean/rainfall/{part.parent.name}/data.parquet"
            lake.put(key, part.read_bytes())
            keys.append(key)
    return keys


ASSESS = """
WITH daily AS (
    SELECT district, province, day, precipitation_mm::DOUBLE AS mm
    FROM read_parquet('{source}', hive_partitioning = false)
),
-- The 7-day total ending on every day, and how many days it really covers.
weekly AS (
    SELECT
        district, province, day,
        sum(mm) OVER w AS total_7d,
        count(*) OVER w AS days_7d
    FROM daily
    WINDOW w AS (
        PARTITION BY district ORDER BY day
        RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
    )
),
current AS (
    SELECT * FROM weekly WHERE day = $day AND days_7d = 7
),
-- The same time of year in every earlier year: weeks ending within 7 days of today's
-- date, so each year gives 15 comparable weeks.
baseline AS (
    SELECT w.district, w.total_7d
    FROM weekly AS w
    WHERE year(w.day) < year($day::DATE)
      AND w.days_7d = 7
      AND least(abs(dayofyear(w.day) - dayofyear($day::DATE)),
                365 - abs(dayofyear(w.day) - dayofyear($day::DATE))) <= 7
),
-- Compare with the baseline and collect the last 7 days in separate steps. Joining both
-- to "current" at once would pair every baseline week with every day and multiply counts.
stats AS (
    SELECT
        c.district,
        median(b.total_7d) AS normal_7d,
        -- Share of comparable weeks that were drier, counting ties as half.
        (count(*) FILTER (WHERE b.total_7d < c.total_7d)
            + 0.5 * count(*) FILTER (WHERE b.total_7d = c.total_7d)) / nullif(count(b.total_7d), 0)
            AS percentile,
        count(b.total_7d) AS baseline_weeks
    FROM current AS c
    LEFT JOIN baseline AS b USING (district)
    GROUP BY c.district, c.total_7d
),
last_week AS (
    SELECT district, list(mm ORDER BY day) AS last_7_days
    FROM daily
    WHERE day BETWEEN $day::DATE - 6 AND $day::DATE
    GROUP BY district
)
SELECT
    c.district, c.province, c.total_7d,
    s.normal_7d, s.percentile, s.baseline_weeks, l.last_7_days
FROM current AS c
JOIN stats AS s USING (district)
JOIN last_week AS l USING (district)
ORDER BY c.district
"""


def observed_level(
    percentile: float | None, baseline_weeks: int, total: float, normal: float | None
) -> str:
    if percentile is None or baseline_weeks < MIN_BASELINE:
        return "Not enough history"
    if percentile >= MUCH_WETTER and total >= 50:
        return "Much wetter than normal"
    if percentile >= WETTER and total >= 25:
        return "Wetter than normal"
    if percentile <= MUCH_DRIER and (normal or 0) >= 25:
        return "Much drier than normal"
    return "Normal"


def forecast_level(max_day_mm: float | None) -> str:
    if max_day_mm is None:
        return "No forecast"
    if max_day_mm >= HEAVY_MM:
        return "Heavy rain likely"
    if max_day_mm >= FAIRLY_HEAVY_MM:
        return "Fairly heavy rain likely"
    return "No heavy rain"


def assess(lake: Lake, districts: list[District], day: date, forecast_key: str | None) -> dict:
    with lake.connect() as conn:
        rows = conn.execute(
            ASSESS.format(source=lake.uri("clean/rainfall/*/data.parquet")).replace("$day", "?"),
            [day] * ASSESS.count("$day"),
        ).fetchall()
    if len(rows) != len(districts):
        raise checks.QualityGateFailed(
            f"assessment has {len(rows)} districts, expected {len(districts)}"
        )

    forecast: dict[str, list[float | None]] = {}
    forecast_days: list[str] = []
    if forecast_key:
        for reading in parse(lake.get(forecast_key), districts):
            forecast.setdefault(reading.district, []).append(reading.precipitation_mm)
            if reading.day.isoformat() not in forecast_days:
                forecast_days.append(reading.day.isoformat())

    out = []
    for district, province, total, normal, percentile, weeks, last_7 in rows:
        ahead = [v for v in forecast.get(district, []) if v is not None]
        max_ahead = max(ahead) if ahead else None
        out.append(
            {
                "district": district,
                "province": province,
                "total_7d": round(total, 1),
                "normal_7d": None if normal is None else round(normal, 1),
                "percentile": None if percentile is None else round(percentile, 3),
                "baseline_weeks": weeks,
                "level": observed_level(percentile, weeks, total, normal),
                "last_7_days": [None if v is None else round(v, 1) for v in last_7],
                "forecast_mm": [
                    None if v is None else round(v, 1) for v in forecast.get(district, [])
                ],
                "forecast_max_mm": max_ahead,
                "forecast_level": forecast_level(max_ahead),
            }
        )
    return {
        "date": day.isoformat(),
        "forecast_days": forecast_days,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": "Open-Meteo.com (CC BY 4.0), from ERA5 reanalysis and national weather services",
        "districts": out,
    }


ALERT_LEVELS = {"Much wetter than normal", "Heavy rain likely"}


def _alerting(assessment: dict | None) -> set[tuple[str, str]]:
    if not assessment:
        return set()
    return {
        (d["district"], level)
        for d in assessment["districts"]
        for level in (d["level"], d["forecast_level"])
        if level in ALERT_LEVELS
    }


def new_alerts(previous: dict | None, current: dict) -> list[tuple[str, str]]:
    """Alerts that weren't already raised in the previous assessment.

    Without this, a wet week would send the same alert every morning, and people stop
    reading alerts that repeat.
    """
    return sorted(_alerting(current) - _alerting(previous))


def send_alerts(alerts: list[tuple[str, str]], day: str) -> str:
    """Post to an ntfy.sh topic if one is configured; anyone subscribed gets a phone alert."""
    if not alerts:
        return "no new alerts"
    text = "\n".join(f"{district}: {level}" for district, level in alerts)
    topic = os.environ.get("RAIN_WATCH_NTFY_TOPIC")
    if not topic:
        return f"no ntfy topic set; would have sent:\n{text}"
    request = urllib.request.Request(
        f"https://ntfy.sh/{topic}",
        data=text.encode(),
        headers={
            "Title": f"rain-watch: {len(alerts)} new alert(s) for {day}",
            "Tags": "cloud_with_rain",
        },
    )
    with urllib.request.urlopen(request, timeout=30):
        pass
    return f"sent {len(alerts)} alert(s)"


def previous_assessment(lake: Lake) -> dict | None:
    if not lake.exists("public/latest.json"):
        return None
    return json.loads(lake.get("public/latest.json"))


def history(lake: Lake, day: date, days: int = 90) -> dict:
    with lake.connect() as conn:
        rows = conn.execute(
            "SELECT district, day, precipitation_mm::DOUBLE "
            f"FROM read_parquet('{lake.uri('clean/rainfall/*/data.parquet')}') "
            "WHERE day > ?::DATE - ? AND day <= ? ORDER BY day",
            [day, days, day],
        ).fetchall()
    dates = sorted({r[1].isoformat() for r in rows})
    series: dict[str, list[float | None]] = {}
    index = {d: i for i, d in enumerate(dates)}
    for district, d, mm in rows:
        series.setdefault(district, [None] * len(dates))[index[d.isoformat()]] = mm
    return {"dates": dates, "districts": series}


def publish(lake: Lake, assessment: dict, site_dir: Path | None) -> list[str]:
    day = date.fromisoformat(assessment["date"])
    files = {
        "latest.json": assessment,
        "history.json": history(lake, day),
    }
    written = []
    for name, value in files.items():
        data = (json.dumps(value, separators=(",", ":")) + "\n").encode()
        lake.put(f"public/{name}", data)
        lake.put(f"public/archive/{day}/{name}", data)
        written.append(f"public/{name}")
        if site_dir:
            site_dir.mkdir(parents=True, exist_ok=True)
            tmp = site_dir / f"{name}.tmp"
            tmp.write_bytes(data)
            os.replace(tmp, site_dir / name)
    return written


def daily_window(day: date) -> tuple[date, date]:
    return day - timedelta(days=REFRESH_DAYS), day
