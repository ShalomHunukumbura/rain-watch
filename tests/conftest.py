"""Made-up Open-Meteo answers, in the same shape as the real ones."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, timedelta

import pytest

from rain_watch.districts import District, load_districts


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def fake_response(
    districts: list[District], start: date, end: date, rain: Callable[[str, date], float | None]
) -> bytes:
    """One entry per district, in request order, as Open-Meteo returns several places."""
    places = []
    for district in districts:
        values = [rain(district.name, d) for d in days(start, end)]
        places.append(
            {
                "latitude": district.latitude,
                "longitude": district.longitude,
                "daily": {
                    "time": [d.isoformat() for d in days(start, end)],
                    "precipitation_sum": values,
                    "precipitation_hours": [
                        None if v is None else (0.0 if v == 0 else 4.0) for v in values
                    ],
                },
            }
        )
    return json.dumps(places).encode()


@pytest.fixture
def districts() -> list[District]:
    return load_districts()
