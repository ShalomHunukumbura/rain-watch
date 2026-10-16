"""The quality gate. A run with any error stops before it writes clean data.

Each check returns a list of problems. "error" stops the run; "warning" is logged and
the run carries on.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta

from .districts import District
from .parse import Reading

# Sri Lanka's wettest days are around 500 mm. 1,000 mm in a day is a broken value.
MAX_DAILY_MM = 1000.0


@dataclass(frozen=True)
class Problem:
    level: str  # "error" or "warning"
    check: str
    detail: str


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def check(
    readings: list[Reading], districts: list[District], start: date, end: date
) -> list[Problem]:
    problems: list[Problem] = []
    expected = {(d.name, day) for d in districts for day in _days(start, end)}
    seen = Counter((r.district, r.day) for r in readings)

    missing = sorted(expected - seen.keys())
    if missing:
        problems.append(
            Problem(
                "error",
                "every district has every day",
                f"{len(missing)} missing, first {missing[0]}",
            )
        )
    extra = sorted(seen.keys() - expected)
    if extra:
        problems.append(
            Problem(
                "error",
                "no unexpected districts or days",
                f"{len(extra)} unexpected, first {extra[0]}",
            )
        )
    duplicates = [key for key, n in seen.items() if n > 1]
    if duplicates:
        problems.append(
            Problem(
                "error",
                "no duplicate rows",
                f"{len(duplicates)} duplicated, first {duplicates[0]}",
            )
        )

    nulls = [r for r in readings if r.precipitation_mm is None or r.precipitation_hours is None]
    if nulls:
        problems.append(
            Problem(
                "error",
                "no missing values",
                f"{len(nulls)} missing, first {nulls[0].district} {nulls[0].day}",
            )
        )

    values = [
        r for r in readings if r.precipitation_mm is not None and r.precipitation_hours is not None
    ]
    bad_mm = [r for r in values if not 0 <= r.precipitation_mm <= MAX_DAILY_MM]
    if bad_mm:
        r = bad_mm[0]
        problems.append(
            Problem(
                "error",
                f"rainfall is between 0 and {MAX_DAILY_MM:.0f} mm",
                f"{len(bad_mm)} rows, first {r.district} {r.day}: {r.precipitation_mm}",
            )
        )
    bad_hours = [r for r in values if not 0 <= r.precipitation_hours <= 24]
    if bad_hours:
        r = bad_hours[0]
        problems.append(
            Problem(
                "error",
                "rain hours are between 0 and 24",
                f"{len(bad_hours)} rows, first {r.district} {r.day}: {r.precipitation_hours}",
            )
        )

    odd = [r for r in values if r.precipitation_mm >= 5 and r.precipitation_hours == 0]
    if odd:
        r = odd[0]
        problems.append(
            Problem(
                "warning",
                "rain has rain hours",
                f"{len(odd)} rows with 5 mm+ in 0 hours, first {r.district} {r.day}",
            )
        )

    # Every district reporting exactly the same rain on a wet day suggests one value was
    # copied everywhere, not 25 real measurements.
    for day in _days(start, end):
        today = {r.precipitation_mm for r in values if r.day == day}
        if len(today) == 1 and next(iter(today)) > 0 and len(districts) > 1:
            problems.append(
                Problem(
                    "error",
                    "districts differ from each other",
                    f"all districts have {next(iter(today))} mm on {day}",
                )
            )
    return problems


def errors(problems: list[Problem]) -> list[Problem]:
    return [p for p in problems if p.level == "error"]


def report(problems: list[Problem]) -> str:
    if not problems:
        return "all checks passed"
    return "\n".join(f"{p.level:8} {p.check}: {p.detail}" for p in problems)


class QualityGateFailed(RuntimeError):
    """Raised to fail the Airflow task, which stops everything after it."""
