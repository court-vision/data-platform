"""The editor's side of a season note: the row a save writes, the entry the
page shows, and a note tried on a morning without touching the one stored."""

from datetime import date, datetime, timedelta

import pytest

from db.models.nba import PlayerProjection, ProjectionAdjustment
from schemas.projections import NoteChange, NoteSave
from services import projection_notes as notes
from services.live_notes import ENDED, IN_FORCE, Note, Standing, standing
from services.live_projection_inputs import LiveInputs, eastern, note_of, noted_inputs, with_notes
from services.live_projection_model import LINE_KEYS, Availability, BoxLine, Prior

pytestmark = pytest.mark.unit

TODAY = date(2026, 11, 16)
AHEAD = [TODAY + timedelta(days=2 * i) for i in range(8)]
WRITTEN = datetime(2026, 11, 16, 9, 0)
STAR, VET = 1, 2
PER_MINUTE = {"pts": 0.6, "reb": 0.2, "ast": 0.15, "stl": 0.03, "blk": 0.02, "tov": 0.06,
              "fgm": 0.22, "fga": 0.46, "fg3m": 0.07, "fg3a": 0.19, "ftm": 0.09, "fta": 0.11}


def _record(**fields) -> ProjectionAdjustment:
    base = dict(id=91, player=VET, season="2026-27", scope="season", kind="role", note="starting", author="jp",
                created_at=datetime(2026, 11, 16, 14, 0), review_date=TODAY + timedelta(days=14))
    return ProjectionAdjustment(**(base | fields))


def _morning(**over) -> LiveInputs:
    lines = [
        BoxLine(player_id=pid, team="DEN", game_id=f"g{g}", game_date=TODAY - timedelta(days=16 - 2 * g),
                minutes=mpg, stats={k: v * mpg for k, v in PER_MINUTE.items()})
        for g in range(8) for pid, mpg in ((STAR, 34.0), (VET, 24.0))
    ]
    priors = {pid: Prior(minutes=mpg, games=70.0, per_game={k: v * mpg for k, v in PER_MINUTE.items()})
              for pid, mpg in ((STAR, 34.0), (VET, 24.0))}
    base = dict(season="2026-27", as_of=TODAY, lines=lines, ahead={"DEN": AHEAD}, priors=priors,
                prior_source="cv", prior_as_of=date(2026, 10, 20), teams={STAR: "DEN", VET: "DEN"}, ages={})
    return LiveInputs(**(base | over))


# ---- what a save writes ------------------------------------------------------------------


def test_a_save_is_a_season_row_read_again_in_two_weeks_unless_it_says_when():
    body = NoteSave(kind="injury_return", return_date=AHEAD[2], return_earliest=AHEAD[1], return_latest=AHEAD[4],
                    minutes=24, usage=0.9, until_date=AHEAD[6], depends_on_player_id=STAR, confidence="likely",
                    note="back on a limit", author="jp")
    evidence = [{"type": "news", "id": 5, "headline": "Vet (hamstring) probable"}]

    assert notes.note_fields(body, TODAY, evidence) == {
        "scope": "season", "kind": "injury_return", "minutes": 24.0, "usage": 0.9, "games": None,
        "return_date": AHEAD[2], "return_earliest": AHEAD[1], "return_latest": AHEAD[4], "until_date": AHEAD[6],
        "depends_on_player": STAR, "evidence": evidence, "confidence": "likely",
        "review_date": TODAY + timedelta(days=notes.DEFAULT_REVIEW_DAYS),
        "note": "back on a limit", "source_url": None, "author": "jp",
    }
    gone = notes.note_fields(
        NoteSave(kind="injury_current", out_for_season=True, review_date=date(2027, 1, 15), note="Achilles"), TODAY, [])
    # A season written off is the table's games = 0; no evidence is null, not an empty list
    assert (gone["games"], gone["review_date"], gone["evidence"]) == (0, date(2027, 1, 15), None)


def test_the_stored_note_is_read_in_eastern_time():
    record = _record(minutes=30, usage=1.1, until_date=AHEAD[3], depends_on_player=STAR,
                     created_at=datetime(2026, 11, 17, 3, 30))          # 10:30 PM on the 16th, Eastern
    note = note_of(record)
    assert note == Note(id=91, player_id=VET, kind="role", written=datetime(2026, 11, 16, 22, 30),
                        review_date=TODAY + timedelta(days=14), minutes=30.0, usage=1.1, until_date=AHEAD[3],
                        depends_on=STAR)
    # In July the same clock is four hours behind
    assert eastern(datetime(2026, 7, 1, 3, 30)) == datetime(2026, 6, 30, 23, 30)


# ---- what the page shows -----------------------------------------------------------------


def test_an_entry_carries_the_notes_own_fields_and_how_it_stands():
    record = _record(
        minutes=30, until_date=AHEAD[3], depends_on_player=STAR, confidence="confirmed",
        evidence=[{"type": "news", "id": 5, "source": "rotowire_rss", "kind": "lineup", "headline": "Vet to start",
                   "url": "https://example.com/5", "published_at": "2026-11-15T22:05:00+00:00"}, "not an item"],
    )
    found = Standing(ENDED, False, False, ("the teammate it depended on played on 2026-11-18",))

    entry = notes.note_entry(record, standing=found, names={STAR: "Star"})

    assert (entry.scope, entry.state, entry.minutes, entry.until_date) == ("season", "live", 30.0, AHEAD[3])
    assert (entry.depends_on_player_id, entry.depends_on_name, entry.confidence) == (STAR, "Star", "confirmed")
    assert entry.review_date == TODAY + timedelta(days=14)
    (cited,) = entry.evidence                                 # what is not an item is left out
    assert (cited.id, cited.source, cited.headline, cited.published_at.isoformat()) == (
        5, "rotowire_rss", "Vet to start", "2026-11-15T22:05:00+00:00")
    assert entry.standing.model_dump() == {
        "state": "ended", "games": False, "role": False,
        "reasons": ["the teammate it depended on played on 2026-11-18"]}


def test_a_preseason_row_is_an_entry_with_none_of_it():
    entry = notes.note_entry(_record(scope="preseason", review_date=None, minutes=33, superseded_by=95))
    assert (entry.scope, entry.state, entry.standing, entry.evidence, entry.review_date) == (
        "preseason", "superseded", None, [], None)
    assert notes.note_entry(_record(retired_at=datetime(2026, 11, 20))).state == "retired"


def test_a_stored_live_row_is_a_line_over_its_unrounded_games():
    row = PlayerProjection(player=VET, season="2026-27", source="cv_live", min=24.6, projected_gp=31,
                           raw={"games": 31.44}, **{k: 1.5 for k in LINE_KEYS})
    line = notes.stored_line(row)
    assert (line.games, line.min, line.pts) == (31.4, 24.6, 1.5)
    # A row with no games in its raw falls back to the rounded column
    assert notes.stored_line(PlayerProjection(player=VET, min=None, projected_gp=12, raw=None)).games == 12.0


# ---- a note tried on ---------------------------------------------------------------------


def test_the_notes_are_laid_over_what_the_feed_knows():
    feed = {STAR: Availability(out_next=True, return_date=AHEAD[1])}
    stored = {STAR: Note(id=3, player_id=STAR, kind="injury_current", written=WRITTEN,
                         review_date=TODAY + timedelta(days=14), return_date=AHEAD[4]),
              VET: Note(id=4, player_id=VET, kind="role", written=WRITTEN, review_date=TODAY + timedelta(days=14),
                        minutes=32.0, depends_on=STAR)}

    out = noted_inputs(feed, stored, TODAY, {})

    # The note's date over the feed's, and the feed still rules him out of the next game
    assert out["known"] == {STAR: Availability(out_next=True, return_date=AHEAD[4], out_now=True)}
    assert set(out["role_notes"]) == {VET} and out["role_notes"][VET].minutes == 32.0
    assert {pid: s.state for pid, s in out["standings"].items()} == {STAR: IN_FORCE, VET: IN_FORCE}
    assert feed[STAR].return_date == AHEAD[1]                 # what the feed said is not written over

    # A season written off has no date, and a note with no games part leaves the feed's reading alone
    gone = Note(id=5, player_id=STAR, kind="injury_current", written=WRITTEN, review_date=AHEAD[7], games=0)
    assert with_notes(feed, {STAR: gone}, {STAR: standing(gone, TODAY, {})})[STAR] == Availability(
        out_next=True, out_for_season=True, out_now=True)
    assert with_notes(feed, {VET: stored[VET]}, {VET: standing(stored[VET], TODAY, {})}) == feed


def test_trying_a_note_replaces_one_players_and_leaves_the_morning_as_it_was():
    have = Note(id=3, player_id=STAR, kind="injury_current", written=WRITTEN,
                review_date=TODAY + timedelta(days=14), return_date=AHEAD[4])
    morning = _morning(known_before_notes={}, **noted_inputs({}, {STAR: have}, TODAY, {}))

    tried = notes.tried_note(VET, NoteChange(minutes=32, depends_on_player_id=STAR), WRITTEN, AHEAD[7])
    assert tried == Note(id=0, player_id=VET, kind="other", written=WRITTEN, review_date=AHEAD[7], minutes=32.0,
                         depends_on=STAR)
    with_it = morning.with_note(VET, tried)

    assert set(with_it.notes) == {STAR, VET} and set(with_it.role_notes) == {VET}
    assert with_it.known == morning.known and with_it.lines is morning.lines
    assert set(morning.notes) == {STAR} and morning.role_notes == {}
    # Taking the star's away un-knows his absence
    without = morning.with_note(STAR, None)
    assert without.notes == {} and without.known == {} and morning.known[STAR].out_now is True


def test_a_preview_is_the_same_morning_with_and_without_the_note():
    have = Note(id=3, player_id=STAR, kind="injury_current", written=WRITTEN,
                review_date=TODAY + timedelta(days=14), return_date=AHEAD[4])
    morning = _morning(known_before_notes={}, **noted_inputs({}, {STAR: have}, TODAY, {}))

    out = notes.preview(morning, VET, NoteChange(minutes=32, usage=1.1, depends_on_player_id=STAR), written=WRITTEN)

    assert (out.player_id, out.as_of) == (VET, TODAY)
    # While the star is out for certain the minutes are the note's; from his return they slide back
    first = out.after.stretches[0]
    assert (first.first, first.last, first.games, first.minutes, first.usage) == (AHEAD[0], AHEAD[3], 4, 32.0, 1.1)
    assert out.after.stretches[-1].minutes < 27 and out.after.line.min > out.before.line.min + 2
    assert out.after.line.games == out.before.line.games      # a role note says nothing about his own games
    assert out.standing.model_dump() == {"state": "in_force", "games": False, "role": True, "reasons": []}
    # The review date defaults as a save's does: past it the note tried would not apply
    assert notes.preview(morning, VET, NoteChange(minutes=32), written=WRITTEN,
                         review_date=TODAY - timedelta(days=1)).standing.state == "review_due"

    # With none: the star as if his note were gone
    star = notes.preview(morning, STAR, None, written=WRITTEN)
    assert star.standing is None and star.before.line.games < star.after.line.games - 3
    assert star.before.stretches[0].chance == 0.0 and len(star.after.stretches) == 1
    # Someone the model does not project
    assert notes.preview(morning, 999, NoteChange(minutes=30), written=WRITTEN) is None
