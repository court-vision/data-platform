"""Season keys: derivation from dates, ESPN year, formatting, and the settings defaults."""

from datetime import date

import pytest
from pydantic import ValidationError

from core import season as s
from core.settings import Settings


@pytest.mark.unit
@pytest.mark.parametrize(
    "d, expected",
    [
        (date(2026, 7, 31), "2025-26"),
        (date(2026, 8, 1), "2026-27"),
        (date(2026, 10, 20), "2026-27"),
        (date(2027, 1, 1), "2026-27"),
        (date(2027, 4, 11), "2026-27"),
        (date(2027, 8, 1), "2027-28"),
        (date(2099, 12, 31), "2099-00"),
    ],
)
def test_season_for_date_flips_on_august_1(d, expected):
    assert s.season_for_date(d) == expected


@pytest.mark.unit
def test_season_key_defaults_to_today(monkeypatch):
    assert s.season_key(date(2026, 8, 25)) == "2026-27"
    assert s.season_key() == s.season_for_date(date.today())


@pytest.mark.unit
def test_derived_helpers():
    assert s.season_from_year(2026) == "2026-27"
    assert s.start_year("2026-27") == 2026
    assert s.espn_year_for("2026-27") == 2027
    assert s.previous_season("2026-27") == "2025-26"
    assert s.next_season("2026-27") == "2027-28"
    assert s.short_key("2026-27") == "26-27"
    assert s.season_label("2026-27") == "2026–27"
    assert s.validate_season("2025-26") == "2025-26"


@pytest.mark.unit
@pytest.mark.parametrize("bad", ["2026-28", "2026-2027", "26-27", "2026/27", "", None])
def test_validate_season_rejects_malformed_keys(bad):
    with pytest.raises(ValueError):
        s.validate_season(bad)


@pytest.mark.unit
def test_settings_derive_season_and_espn_year_from_today(monkeypatch):
    monkeypatch.delenv("NBA_SEASON", raising=False)
    monkeypatch.delenv("ESPN_YEAR", raising=False)
    cfg = Settings(_env_file=None)
    assert cfg.nba_season == s.season_key()
    assert cfg.espn_year == s.espn_year_for(cfg.nba_season)


@pytest.mark.unit
def test_settings_env_overrides_remain(monkeypatch):
    monkeypatch.setenv("NBA_SEASON", "2024-25")
    monkeypatch.delenv("ESPN_YEAR", raising=False)
    cfg = Settings(_env_file=None)
    assert (cfg.nba_season, cfg.espn_year) == ("2024-25", 2025)

    monkeypatch.setenv("ESPN_YEAR", "2031")
    cfg = Settings(_env_file=None)
    assert (cfg.nba_season, cfg.espn_year) == ("2024-25", 2031)


@pytest.mark.unit
def test_settings_reject_malformed_season(monkeypatch):
    monkeypatch.setenv("NBA_SEASON", "2026-2027")
    with pytest.raises(ValidationError):
        Settings(_env_file=None)
