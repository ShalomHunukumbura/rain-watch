"""Turns Open-Meteo's JSON into rows, and checks the response has the shape we expect."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date

from .districts import District


class ParseError(ValueError):
    pass


@dataclass(frozen=True)
class Reading:
    district: str
    day: date
    precipitation_mm: float | None
    precipitation_hours: float | None


def parse(raw: bytes, districts: list[District]) -> list[Reading]:
    places = json.loads(raw)
    if isinstance(places, dict):
        if places.get("error"):
            raise ParseError(f"Open-Meteo error: {places.get('reason')}")
        places = [places]
    # With several places, Open-Meteo answers with a list in the order we asked.
    if len(places) != len(districts):
        raise ParseError(f"asked for {len(districts)} places, got {len(places)}")
    readings: list[Reading] = []
    for district, place in zip(districts, places, strict=True):
        daily = place.get("daily") or {}
        days = daily.get("time")
        mm = daily.get("precipitation_sum")
        hours = daily.get("precipitation_hours")
        if days is None or mm is None or hours is None or not len(days) == len(mm) == len(hours):
            raise ParseError(f"{district.name}: daily values missing or of different lengths")
        readings += [
            Reading(district.name, date.fromisoformat(d), m, h)
            for d, m, h in zip(days, mm, hours, strict=True)
        ]
    return readings
