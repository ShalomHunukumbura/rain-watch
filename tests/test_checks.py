from datetime import date

from conftest import fake_response

from rain_watch.checks import check, errors
from rain_watch.parse import parse

DAY = date(2026, 10, 15)


def problems(districts, rain, start=DAY, end=DAY):
    raw = fake_response(districts, start, end, rain)
    return {p.check for p in errors(check(parse(raw, districts), districts, start, end))}


def test_good_data_passes(districts):
    assert problems(districts, lambda name, d: float(len(name))) == set()


def test_a_missing_value_fails(districts):
    rain = lambda name, d: None if name == "Kandy" else 3.0  # noqa: E731
    assert "no missing values" in problems(districts, rain)


def test_impossible_rain_fails(districts):
    rain = lambda name, d: 5000.0 if name == "Galle" else 3.0  # noqa: E731
    assert "rainfall is between 0 and 1000 mm" in problems(districts, rain)


def test_negative_rain_fails(districts):
    rain = lambda name, d: -1.0 if name == "Galle" else 3.0  # noqa: E731
    assert "rainfall is between 0 and 1000 mm" in problems(districts, rain)


def test_the_same_value_everywhere_fails(districts):
    assert "districts differ from each other" in problems(districts, lambda name, d: 12.5)


def test_a_dry_day_everywhere_is_fine(districts):
    assert problems(districts, lambda name, d: 0.0) == set()


def test_a_missing_day_fails(districts):
    # Asked for two days, the answer has one.
    raw = fake_response(districts, DAY, DAY, lambda name, d: float(len(name)))
    found = check(parse(raw, districts), districts, DAY, date(2026, 10, 16))
    assert "every district has every day" in {p.check for p in errors(found)}
