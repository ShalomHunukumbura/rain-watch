"""Fetches daily rainfall from Open-Meteo for all districts in one request.

Free, no key, non-commercial use only, and the data must be credited (CC BY 4.0).

One request can ask for many places. Open-Meteo counts each place as a call, and a
request longer than two weeks as proportionally more, so a month for 25 districts costs
about 55 of the 10,000 calls a day allows.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date

from .districts import District

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
DAILY = "precipitation_sum,precipitation_hours"
TIMEZONE = "Asia/Colombo"
USER_AGENT = os.environ.get(
    "RAIN_WATCH_USER_AGENT",
    "rain-watch/0.1 (+https://github.com/ShalomHunukumbura/rain-watch)",
)


class SourceError(RuntimeError):
    """The request failed. Airflow retries the task."""


class RateLimited(SourceError):
    """Open-Meteo said "too many requests" (HTTP 429)."""


def _get(url: str, params: dict, attempts: int = 3) -> bytes:
    full = f"{url}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(full, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code == 429:
                # Waiting seconds won't help with an hourly or daily limit. Fail, and let
                # Airflow retry the task much later.
                raise RateLimited(f"rate limited: {error.read()[:200]!r}") from error
            if error.code < 500 or attempt == attempts:
                raise SourceError(f"{full} -> HTTP {error.code}: {error.read()[:200]!r}") from error
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt == attempts:
                raise SourceError(f"{full} failed: {error}") from error
        time.sleep(5 * attempt)
    raise AssertionError("unreachable")


def _places(districts: list[District]) -> dict:
    return {
        "latitude": ",".join(f"{d.latitude:.4f}" for d in districts),
        "longitude": ",".join(f"{d.longitude:.4f}" for d in districts),
    }


def fetch_observed(districts: list[District], start: date, end: date) -> bytes:
    """Rainfall that fell, one value per district per day, from start to end inclusive."""
    return _get(
        ARCHIVE_URL,
        {
            **_places(districts),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "daily": DAILY,
            "timezone": TIMEZONE,
        },
    )


def fetch_forecast(districts: list[District], days: int = 3) -> bytes:
    """Forecast rainfall for today and the next days."""
    return _get(
        FORECAST_URL,
        {
            **_places(districts),
            "daily": DAILY,
            "timezone": TIMEZONE,
            "forecast_days": days,
        },
    )


def has_all_values(raw: bytes, day: date) -> bool:
    """Is rainfall for this day published for every district yet?"""
    places = json.loads(raw)
    places = places if isinstance(places, list) else [places]
    for place in places:
        daily = place["daily"]
        if day.isoformat() not in daily["time"]:
            return False
        if daily["precipitation_sum"][daily["time"].index(day.isoformat())] is None:
            return False
    return True
