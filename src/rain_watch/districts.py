"""The 25 districts, each represented by its main town."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from importlib.resources import files


@dataclass(frozen=True)
class District:
    name: str
    province: str
    town: str
    latitude: float
    longitude: float


def load_districts() -> list[District]:
    with files(__package__).joinpath("districts.csv").open(newline="") as f:
        return [
            District(
                row["district"],
                row["province"],
                row["town"],
                float(row["latitude"]),
                float(row["longitude"]),
            )
            for row in csv.DictReader(f)
        ]
