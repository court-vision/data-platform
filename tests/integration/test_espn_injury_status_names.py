"""espn_injury_status's name fallback, for a player ESPN's id does not resolve.

nba.players also holds every player since 2012-13 (the game-history load). The
fallback knows only today's players (Player.in_play) and only a name one of
them holds alone: an injury on a retired namesake, or on the wrong one of two
current players, is worse than none.
"""

from __future__ import annotations

import pytest

from core.settings import settings
from db.models.nba.player_profiles import PlayerProfile
from db.models.nba.players import Player
from pipelines.espn_injury_status import ESPNInjuryStatusPipeline

pytestmark = pytest.mark.integration


def _player(pid: int, name: str, to_year: int | None) -> None:
    Player.create(id=pid, name=name, name_normalized=name.lower())
    PlayerProfile.create(player=pid, to_year=to_year)


def test_the_name_fallback_knows_current_players_with_a_name_of_their_own(monkeypatch):
    monkeypatch.setattr(settings, "nba_season", "2026-27")  # last season's PlayerIndex year: 2025
    _player(1630162, "Anthony Edwards", to_year=2025)
    _player(1, "Anthony Edwards", to_year=2014)   # a retired namesake: his profile has aged out
    _player(1631114, "Jalen Williams", to_year=2025)
    _player(1631116, "Jalen Williams", to_year=None)  # two current players: the name is neither's

    assert ESPNInjuryStatusPipeline()._build_name_lookup() == {"anthony edwards": 1630162}
