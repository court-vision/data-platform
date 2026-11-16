"""
Fitting and backtesting the live projection (services.live_projection_model).

Pure: box scores and preseason projections in, coefficients or metrics out.
`scripts/fit_live_projection.py` is the entry point; it reads
nba.player_game_stats (or a directory of CSVs) and writes
`static/live_projection_coefficients.json`.

**What a season teaches.** Each past season is stopped on three mornings (26,
45 and 70 days after opening night). On each, the model sees what it would have
seen that day, the box scores so far and the preseason projection, and the rest
of the season is what it is scored on. One piece of hindsight goes in: a
projected player who had not played yet is placed on the team he later debuted
for, which the day's rosters would have said and the box scores cannot.

**The preseason projection** of a past season is the statistical half of the
one the board uses (services.projection_model, three seasons of history). ESPN's
half was never archived.

**Fitted in this order,** each on the players with at least five games before
the morning and ten after it:

1. *Games.* For every regular, by how many games he had just missed: whether he
   played each of his team's next seventy, which is the curve; and the share of
   games he played once back, with a least-squares lean of that share on his
   projected games and on this season's attendance.
2. *Prior weights.* For each per-minute volume and each shooting percentage, the
   weight of the projection that best predicts the rest of the season, from a
   grid. Weights within `TOLERANCE` of the best are ties, and the tie goes to
   the smallest: where the backtest cannot separate two weights, this season's
   games win.
3. *Minutes.* The projection's weight in games, the half-life, the situation
   slope and how long it outweighs a player's own games, by coordinate descent
   on the error in minutes.
4. *Usage and development,* on the error in fantasy points a game.

**The backtest** (`leave_one_out`) refits everything on seven seasons and scores
the eighth, for each season in turn, against the blend it replaces: the same
projection and this season's per-game line at ten games' weight, with the
projection's games share applied to the games left (backend
`services.draft_in_season`).
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Iterable, Mapping, Optional, Sequence

from services.live_projection_model import (
    BUCKETS,
    HORIZON,
    LINE_KEYS,
    PCT_PAD,
    LEAGUE_PCT,
    PRE_DEBUT,
    SHOOTING,
    VOLUME_KEYS,
    BoxLine,
    Coefficients,
    Morning,
    Outlook,
    PlayerState,
    Prior,
    _with_twos,
    average_line,
    bucket,
    build_morning,
    development_bucket,
    generic_prior,
    minutes_for,
    outlooks,
    prior_totals,
    project_player,
    role_of,
)
from services.projection_fit import POINTS, _least_squares, _spearman
from services.projection_model import Coefficients as SeasonCoefficients
from services.projection_model import HistoryRow, cell_means, role_cells, statistical_line

CUTOFFS: tuple[int, ...] = (26, 45, 70)
TOLERANCE = 0.002
MIN_BEFORE, MIN_AFTER = 5, 10               # games each side of the morning, to be fitted on
BLEND_GAMES = 10.0                          # the blend this replaces (backend SEASON_PRIOR_GAMES)
BOARD = 156                                 # twelve teams of thirteen
SAMPLE_TOP, SAMPLE_GAMES = 200, 10          # the players the blend was fitted and scored on

GRID_VOLUME = (50, 100, 150, 200, 300, 400, 600, 800, 1200, 1800, 2700, 4000)
GRID_SHOOTING = (25, 50, 100, 150, 200, 300, 450, 700, 1000, 1500, 2500)
GRID_MINUTES = {
    "minutes_prior_games": (1, 2, 3, 4, 5, 6, 8),
    "minutes_half_life": (4, 6, 8, 10, 14, 20, 40),
    "situation_slope": (0.0, 0.02, 0.04, 0.06, 0.08, 0.10, 0.13),
    "own_slope_ridge": (0.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0),
}
GRID_USAGE = (0.0, 0.0015, 0.003, 0.0045, 0.006)
GRID_DEVELOPMENT = (0.0, 0.01, 0.02, 0.03, 0.04, 0.06, 0.08)

START = Coefficients(
    plays=(), played_once_back=(), lean_prior_share=0.0, lean_attendance=0.0,
    mean_prior_share=0.0, mean_attendance=0.0,
    minutes_prior_games=3.0, minutes_half_life=10.0, situation_slope=0.08, own_slope_ridge=8000.0,
    usage_slope=0.003, development={},
    prior_weights={k: 400.0 for k in VOLUME_KEYS} | {"fg2": 300.0, "fg3": 300.0, "ft": 150.0},
)


def fpts(line: Mapping[str, float]) -> float:
    return sum(w * line.get(k, 0.0) for k, w in POINTS.items())


# ---- the preseason projection of a past season -------------------------------------------


def history_rows(
    lines_by_season: Mapping[int, Sequence[BoxLine]],
    bio: Mapping[tuple[int, int], tuple[Optional[float], Optional[int]]],
) -> dict[int, list[HistoryRow]]:
    """Season totals from box scores, shaped as nba.player_history rows.

    `bio` is (player, season) -> (age that season, first NBA season). A line's
    stats may carry oreb and dreb beside the line's own keys: the role cells of
    the preseason projection read them.
    """
    keys = LINE_KEYS + ("oreb", "dreb")
    out: dict[int, list[HistoryRow]] = {}
    for season, lines in lines_by_season.items():
        acc: dict[int, dict[str, float]] = {}
        for l in lines:
            a = acc.setdefault(l.player_id, {"gp": 0.0, "min": 0.0})
            a["gp"] += 1
            a["min"] += l.minutes
            for k in keys:
                a[k] = a.get(k, 0.0) + float(l.stats.get(k, 0.0))
        for pid, a in acc.items():
            age, first = bio.get((pid, season), (None, None))
            out.setdefault(pid, []).append(HistoryRow(
                player_id=pid, season=season, gp=int(a["gp"]), minutes=a["min"],
                totals={k: a.get(k, 0.0) for k in keys}, age=age, from_year=first,
            ))
    return out


def season_priors(
    history: Mapping[int, Sequence[HistoryRow]], target: int, coeffs: SeasonCoefficients
) -> dict[int, Prior]:
    """The statistical preseason projection of the season starting `target`, per player."""
    window = {pid: [r for r in rows if target - 3 <= r.season <= target - 1 and r.minutes > 0]
              for pid, rows in history.items()}
    window = {pid: rows for pid, rows in window.items() if rows}
    cells = role_cells(window)
    by_cell, league = cell_means(window, cells, target - 1)
    out: dict[int, Prior] = {}
    for pid, rows in window.items():
        p = statistical_line(rows, target, coeffs, by_cell.get(cells.get(pid), league))
        if p is not None:
            out[pid] = Prior(minutes=p.minutes, games=p.games, per_game={k: p.per_game[k] for k in LINE_KEYS})
    return out


def ages_for(history: Mapping[int, Sequence[HistoryRow]], target: int) -> dict[int, float]:
    """Each player's age in the season starting `target`, from the season before."""
    out: dict[int, float] = {}
    for pid, rows in history.items():
        for r in rows:
            if r.season == target - 1 and r.age is not None:
                out[pid] = r.age + 1.0
    return out


def last_season_ranks(history: Mapping[int, Sequence[HistoryRow]], target: int) -> dict[int, int]:
    """Players by last season's fantasy points a game, best first: who the blend was scored on."""
    per_game = {
        pid: fpts(r.totals) / r.gp
        for pid, rows in history.items() for r in rows if r.season == target - 1 and r.gp > 0
    }
    return {pid: i for i, pid in enumerate(sorted(per_game, key=lambda p: -per_game[p]))}


def sat_out_season(
    lines_by_season: Mapping[int, Sequence[BoxLine]], season: int
) -> dict[int, str]:
    """Players with no game in `season` who played the season before and the
    season after, and the team they last played for: a year lost to injury far
    more often than anything else. The box scores cannot tell a player on a
    roster and hurt from one out of the league, and this is the nearest they get."""
    if season - 1 not in lines_by_season or season + 1 not in lines_by_season:
        return {}
    played = {l.player_id for l in lines_by_season[season]}
    came_back = {l.player_id for l in lines_by_season[season + 1]}
    last: dict[int, tuple[date, str]] = {}
    for l in lines_by_season[season - 1]:
        if l.player_id not in last or l.game_date >= last[l.player_id][0]:
            last[l.player_id] = (l.game_date, l.team)
    return {pid: team for pid, (_day, team) in last.items() if pid not in played and pid in came_back}


# ---- samples: one morning of one past season ------------------------------------------------


@dataclass(frozen=True)
class After:
    """What a player did from the morning on."""

    games: int
    minutes: float
    totals: Mapping[str, float]

    def per_game(self, key: str) -> float:
        return (self.minutes if key == "min" else self.totals.get(key, 0.0)) / self.games


@dataclass
class Sample:
    season: int
    days: int
    morning: Morning
    after: dict[int, After]
    back_on: dict[int, date]                    # the day each player next played
    played_on: dict[int, set[date]]             # every day he played from the morning on
    ranked: dict[int, int]
    looks: dict[int, Outlook] = field(default_factory=dict)

    def attach(self, coeffs: Coefficients) -> "Sample":
        self.looks = outlooks(self.morning, coeffs)
        return self


def build_sample(
    season: int,
    days: int,
    lines: Sequence[BoxLine],
    priors: Mapping[int, Prior],
    ages: Optional[Mapping[int, float]] = None,
    ranked: Optional[Mapping[int, int]] = None,
    sat_out: Optional[Mapping[int, str]] = None,
) -> Sample:
    """`season` as it stood `days` after opening night, and what came after.

    `sat_out` is projected players who missed the whole season and the team they
    belonged to (see `sat_out_season`): without them every player who has not
    played yet would be one who is about to, and the return curve of that bucket
    would promise a return to players who are out for the year.
    """
    cutoff = min(l.game_date for l in lines) + timedelta(days=days)
    later = sorted((l for l in lines if l.game_date >= cutoff), key=lambda l: (l.game_date, l.game_id))
    seen = {l.player_id for l in lines if l.game_date < cutoff}
    ahead: dict[str, set[date]] = {}
    after: dict[int, dict] = {}
    back_on: dict[int, date] = {}
    played_on: dict[int, set[date]] = {}
    debut: dict[int, str] = {}
    for l in later:
        ahead.setdefault(l.team, set()).add(l.game_date)
        played_on.setdefault(l.player_id, set()).add(l.game_date)
        a = after.setdefault(l.player_id, {"games": 0, "minutes": 0.0, "totals": {k: 0.0 for k in LINE_KEYS}})
        a["games"] += 1
        a["minutes"] += l.minutes
        for k in LINE_KEYS:
            a["totals"][k] += float(l.stats.get(k, 0.0))
        if l.player_id not in back_on:
            back_on[l.player_id] = l.game_date
            if l.player_id not in seen and l.player_id in priors:
                debut[l.player_id] = l.team
    everyone = {l.player_id for l in lines}
    for pid, team in (sat_out or {}).items():
        if pid not in everyone and pid in priors:
            debut[pid] = team
    morning = build_morning(
        lines, priors, {team: sorted(d) for team, d in ahead.items()}, cutoff, teams=debut, ages=ages,
    )
    return Sample(
        season=season, days=days, morning=morning,
        after={pid: After(a["games"], a["minutes"], a["totals"]) for pid, a in after.items()},
        back_on=back_on, played_on=played_on, ranked=dict(ranked or {}),
    )


# ---- games --------------------------------------------------------------------------------------


CURVE_MIN_PLAYERS = 30          # a point of a curve measured on fewer is carried from the one before
CURVE_SMOOTH = 2                # games each side averaged into a point


def fit_games(samples: Iterable[Sample], coeffs: Coefficients = START) -> Coefficients:
    """The curves (who plays the k-th game from now), the share played once back and its two leans."""
    did = [[0.0] * (HORIZON + 1) for _ in range(BUCKETS)]
    n_at = [[0.0] * (HORIZON + 1) for _ in range(BUCKETS)]
    played = [0.0] * BUCKETS
    could = [0.0] * BUCKETS
    X: list[list[float]] = []
    y: list[float] = []
    for s in samples:
        for team, members in s.morning.rosters.items():
            days = s.morning.ahead.get(team, [])
            reach = min(len(days), HORIZON)
            for m in members:
                b = PRE_DEBUT if m.pre else bucket(m.streak)
                # A slot is the days up to one of this team's games since its
                # game before: a game he played for another team (after a
                # trade) fills the slot it falls in, at most one a slot.
                on = sorted(s.played_on.get(m.player_id, ()))
                before = 0
                for j in range(1, reach + 1):
                    upto = bisect_right(on, days[j - 1])
                    n_at[b][j] += 1
                    if upto > before:
                        did[b][j] += 1
                    before = upto
                back = s.back_on.get(m.player_id)
                if back is None:
                    continue
                # the game of this team's he is back for, counted from 1
                k = max(bisect_right(days, back), 1)
                if k > len(days):
                    continue
                left = len(days) - k + 1
                got = min(s.after[m.player_id].games if m.player_id in s.after else 0, left)
                could[b] += left
                played[b] += got
                state = s.morning.players.get(m.player_id)
                if state is not None and state.prior is not None and state.n >= MIN_BEFORE:
                    X.append([1.0, state.prior.share, state.n / max(state.team_played, 1)])
                    y.append(got / left)
    overall = sum(played) / sum(could) if sum(could) else 0.8
    once_back = [played[b] / could[b] if could[b] else overall for b in range(BUCKETS)]
    plays: list[list[float]] = []
    for b in range(BUCKETS):
        measured = [0.0] * (HORIZON + 1)
        last = once_back[b] if b == 0 else 0.0
        for j in range(1, HORIZON + 1):
            if n_at[b][j] >= CURVE_MIN_PLAYERS:
                last = did[b][j] / n_at[b][j]
            measured[j] = last
        row = [0.0] * (HORIZON + 1)
        for j in range(1, HORIZON + 1):
            half = min(CURVE_SMOOTH, j - 1, HORIZON - j)
            window = measured[j - half:j + half + 1]
            row[j] = sum(window) / len(window)
        plays.append(row)
    beta = _least_squares(X, y) if X else [overall, 0.0, 0.0]
    n = max(len(X), 1)
    return replace(
        coeffs,
        plays=tuple(tuple(r) for r in plays), played_once_back=tuple(once_back),
        lean_prior_share=beta[1], lean_attendance=beta[2],
        mean_prior_share=sum(r[1] for r in X) / n, mean_attendance=sum(r[2] for r in X) / n,
    )


# ---- skill, role -----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Record:
    """One player on one morning, with what he did after it."""

    sample: Sample
    state: PlayerState
    look: Outlook
    after: After
    generic: Optional[Mapping[str, float]]

    @property
    def in_sample(self) -> bool:
        """Scored the way the blend was: top 200 by last season's line, ten games each side."""
        return (self.sample.ranked.get(self.state.player_id, 10 ** 6) < SAMPLE_TOP
                and self.state.n >= SAMPLE_GAMES and self.after.games >= SAMPLE_GAMES)


def records(samples: Iterable[Sample], fitted_on: bool = True) -> list[Record]:
    """Every projected player with a line after the morning; `fitted_on` keeps
    those with enough games on both sides to be fitted on."""
    out: list[Record] = []
    for s in samples:
        generic = generic_prior(s.morning)
        for pid, state in s.morning.players.items():
            after = s.after.get(pid)
            if state.prior is None or after is None or pid not in s.looks:
                continue
            if fitted_on and not (state.n >= MIN_BEFORE and after.games >= MIN_AFTER):
                continue
            out.append(Record(sample=s, state=state, look=s.looks[pid], after=after, generic=generic))
    return out


def _smallest_within(grid: Sequence[float], losses: Sequence[float]) -> float:
    best = min(losses)
    return float(next(g for g, loss in zip(grid, losses) if loss <= best * (1.0 + TOLERANCE)))


def fit_prior_weights(pool: Sequence[Record]) -> dict[str, float]:
    """One weight of the projection per stat: minutes for the per-minute volumes,
    attempts for the percentages, scored on the rest of the season's rate."""
    season = [_with_twos(r.state.totals) for r in pool]
    prior = [prior_totals(r.state.prior) for r in pool]
    after = [_with_twos(dict(r.after.totals)) for r in pool]
    after_min = [max(r.after.minutes, 1.0) for r in pool]
    weights: dict[str, float] = {}
    for k in VOLUME_KEYS:
        losses = []
        for g in GRID_VOLUME:
            loss = 0.0
            for s, p, a, m in zip(season, prior, after, after_min):
                rate = (s[k] + g * p[k] / max(p["min"], 1.0)) / (s["min"] + g)
                loss += m * (rate - a[k] / m) ** 2
            losses.append(loss / sum(after_min))
        weights[k] = _smallest_within(GRID_VOLUME, losses)
    for name, (made, tried) in SHOOTING.items():
        rows = [(s, p, a) for s, p, a in zip(season, prior, after) if a[tried] > 0]
        total = sum(a[tried] for _s, _p, a in rows)
        losses = []
        for g in GRID_SHOOTING:
            loss = 0.0
            for s, p, a in rows:
                before = (p[made] + PCT_PAD * LEAGUE_PCT[name]) / (p[tried] + PCT_PAD)
                pct = (s[made] + g * before) / (s[tried] + g)
                loss += a[tried] * (pct - a[made] / a[tried]) ** 2
            losses.append(loss / total)
        weights[name] = _smallest_within(GRID_SHOOTING, losses)
    return weights


def _minutes_error(pool: Sequence[Record], coeffs: Coefficients) -> float:
    total = 0.0
    for r in pool:
        minutes = minutes_for(role_of(r.state, coeffs), r.state, r.look.average(r.look.missing), coeffs)
        total += abs(minutes - r.after.minutes / r.after.games)
    return total / len(pool)


def fit_minutes(pool: Sequence[Record], coeffs: Coefficients, sweeps: int = 2) -> Coefficients:
    """Coordinate descent over the four minutes coefficients. For the projection's
    weight and the half-life a tie goes to this season: the smaller value."""
    for _ in range(sweeps):
        for name, grid in GRID_MINUTES.items():
            losses = [_minutes_error(pool, replace(coeffs, **{name: float(g)})) for g in grid]
            if name in ("minutes_prior_games", "minutes_half_life"):
                pick = _smallest_within(grid, losses)
            else:
                pick = float(grid[losses.index(min(losses))])
            coeffs = replace(coeffs, **{name: pick})
    return coeffs


def _fpts_error(pool: Sequence[Record], coeffs: Coefficients) -> float:
    total = 0.0
    for r in pool:
        _minutes, line = average_line(r.state, r.look, coeffs, r.generic)
        total += abs(fpts(line) - fpts(r.after.totals) / r.after.games)
    return total / len(pool)


def fit_usage_and_development(pool: Sequence[Record], coeffs: Coefficients) -> Coefficients:
    losses = [_fpts_error(pool, replace(coeffs, usage_slope=u)) for u in GRID_USAGE]
    coeffs = replace(coeffs, usage_slope=GRID_USAGE[losses.index(min(losses))])
    development: dict[str, float] = {}
    for name in ("u22", "u24"):
        young = [r for r in pool if development_bucket(r.state.age) == name]
        if len(young) < 40:
            continue
        losses = [_fpts_error(young, replace(coeffs, development=development | {name: d})) for d in GRID_DEVELOPMENT]
        development[name] = GRID_DEVELOPMENT[losses.index(min(losses))]
    return replace(coeffs, development=development)


def fit(samples: Sequence[Sample]) -> Coefficients:
    """Every coefficient, from `samples`. Leaves each sample's outlooks attached."""
    coeffs = fit_games(samples)
    for s in samples:
        s.attach(coeffs)
    pool = records(samples)
    coeffs = replace(coeffs, prior_weights=fit_prior_weights(pool))
    coeffs = fit_minutes(pool, coeffs)
    return fit_usage_and_development(pool, coeffs)


# ---- the backtest -------------------------------------------------------------------------------------


def blend_line(state: PlayerState, games: float = BLEND_GAMES) -> dict[str, float]:
    """The line this model replaces: the projection and this season's per-game
    line, the projection worth `games` games."""
    n = state.n
    out = {k: (state.totals.get(k, 0.0) + games * state.prior.per_game.get(k, 0.0)) / (n + games) for k in LINE_KEYS}
    out["min"] = (state.totals.get("min", 0.0) + games * state.prior.minutes) / (n + games)
    return out


def seat_value(per_game: float, games: float, team_games: float, replacement: float) -> float:
    """What a roster seat is worth with him in it: his games at his line, and the
    games he misses at a replacement's (never more than his own)."""
    return per_game * games + min(per_game, replacement) * max(team_games - games, 0.0)


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def score(samples: Sequence[Sample], coeffs: Coefficients) -> dict:
    """The live projection against the blend on `samples` (outlooks attached)."""
    errors: dict[str, list[float]] = {k: [] for k in ("prior", "season", "blend", "live")}
    stat_blend: dict[str, list[float]] = {}
    stat_live: dict[str, list[float]] = {}
    boards: list[dict[str, float]] = []
    for s in samples:
        rows = []
        for r in records([s], fitted_on=False):
            live = project_player(r.state, r.look, s.morning.ahead.get(r.state.team, []), coeffs, r.generic)
            blend = blend_line(r.state)
            left = len(s.morning.ahead.get(r.state.team, []))
            actual = fpts(r.after.totals)
            rows.append({
                "n": r.state.n, "left": left, "after": r.after.games, "actual": actual,
                "blend": fpts(blend), "blend_games": r.state.prior.share * left,
                "live": fpts(live.per_game), "live_games": live.games,
            })
            if not r.in_sample:
                continue
            per_game = actual / r.after.games
            errors["prior"].append(abs(fpts(r.state.prior.per_game) - per_game))
            errors["season"].append(abs(fpts(r.state.totals) / r.state.n - per_game))
            errors["blend"].append(abs(fpts(blend) - per_game))
            errors["live"].append(abs(fpts(live.per_game) - per_game))
            for k in ("min", "pts", "reb", "ast", "stl", "blk", "tov", "fg3m"):
                want = r.after.per_game(k)
                stat_blend.setdefault(k, []).append(abs(blend[k] - want))
                stat_live.setdefault(k, []).append(abs((live.minutes if k == "min" else live.per_game[k]) - want))
            for name, made, tried in (("fg_pct", "fgm", "fga"), ("ft_pct", "ftm", "fta")):
                if r.after.totals[tried] > 0:
                    want = r.after.totals[made] / r.after.totals[tried]
                    stat_blend.setdefault(name, []).append(abs(blend[made] / max(blend[tried], 1e-9) - want))
                    stat_live.setdefault(name, []).append(
                        abs(live.per_game[made] / max(live.per_game[tried], 1e-9) - want))
        # players of this morning's projection who never played again are on the board with nothing after
        for pid, state in s.morning.players.items():
            if state.prior is None or pid in s.after or pid not in s.looks:
                continue
            left = len(s.morning.ahead.get(state.team, []))
            live = project_player(state, s.looks[pid], s.morning.ahead.get(state.team, []), coeffs)
            rows.append({"n": state.n, "left": left, "after": 0, "actual": 0.0,
                         "blend": fpts(blend_line(state)), "blend_games": state.prior.share * left,
                         "live": fpts(live.per_game), "live_games": live.games})
        boards.append(_board(rows))
    count = len(errors["live"])
    out = {
        "players": count,
        "fpts_pg_mae": {k: round(_mean(v), 3) for k, v in errors.items()},
        "per_stat_mae": {k: {"blend": round(_mean(stat_blend[k]), 4), "live": round(_mean(stat_live[k]), 4)}
                         for k in stat_blend},
    }
    for key in boards[0] if boards else ():
        out[key] = round(_mean([b[key] for b in boards]), 4)
    return out


def _board(rows: Sequence[Mapping[str, float]]) -> dict[str, float]:
    """Two orders of one morning's players against what the rest of the season was worth."""
    def replacement(key: str) -> float:
        ranked = sorted((r[key] for r in rows if r["n"] >= MIN_BEFORE), reverse=True)
        return ranked[BOARD] if len(ranked) > BOARD else 0.0

    level = replacement("blend")
    blend_order = [r["blend"] * r["blend_games"] for r in rows]
    live_level = replacement("live")
    live_order = [seat_value(r["live"], r["live_games"], r["left"], live_level) for r in rows]
    over = [r["actual"] - level * r["after"] for r in rows]             # value over a replacement
    worth = [max(v, 0.0) for v in over]
    points = [r["actual"] for r in rows]

    def top(order: Sequence[float], size: int = BOARD) -> list[int]:
        return sorted(range(len(order)), key=lambda i: -order[i])[:size]

    best_over = sum(sorted(worth, reverse=True)[:BOARD]) or 1.0
    best_points = sum(sorted(points, reverse=True)[:BOARD]) or 1.0
    union = sorted(set(top(blend_order, 200)) | set(top(live_order, 200)))
    return {
        "board_value_blend": sum(worth[i] for i in top(blend_order)) / best_over,
        "board_value_live": sum(worth[i] for i in top(live_order)) / best_over,
        "board_points_blend": sum(points[i] for i in top(blend_order)) / best_points,
        "board_points_live": sum(points[i] for i in top(live_order)) / best_points,
        "board_rho_blend": _spearman([blend_order[i] for i in union], [over[i] for i in union]),
        "board_rho_live": _spearman([live_order[i] for i in union], [over[i] for i in union]),
        "games_mae_blend": _mean([abs(rows[i]["blend_games"] - rows[i]["after"]) for i in union]),
        "games_mae_live": _mean([abs(rows[i]["live_games"] - rows[i]["after"]) for i in union]),
    }


def leave_one_out(by_season: Mapping[int, Sequence[Sample]]) -> dict:
    """Fit on every season but one and score that one, for each season in turn.
    Returns the scores by days after opening night, each season weighted by its players."""
    folds = [hold_out(by_season, season) for season in sorted(by_season)]
    return combine(folds)


def hold_out(by_season: Mapping[int, Sequence[Sample]], season: int) -> dict:
    """One fold: fitted without `season`, scored on it."""
    train = [s for y, group in by_season.items() if y != season for s in group]
    coeffs = fit(train)
    out: dict = {"season": season, "by_days": {}}
    for s in by_season[season]:
        s.attach(coeffs)
        out["by_days"][s.days] = score([s], coeffs)
    out["coefficients"] = {k: v for k, v in coeffs.to_json().items() if k != "games"}
    return out


def combine(folds: Sequence[Mapping]) -> dict:
    out: dict = {}
    days = sorted({d for f in folds for d in f["by_days"]})
    for d in days:
        parts = [f["by_days"][d] for f in folds if d in f["by_days"]]
        players = sum(p["players"] for p in parts)

        def weighted(pick) -> float:
            return round(sum(pick(p) * p["players"] for p in parts) / max(players, 1), 4)

        entry: dict = {
            "players": players,
            "fpts_pg_mae": {k: weighted(lambda p, k=k: p["fpts_pg_mae"][k]) for k in ("prior", "season", "blend", "live")},
            "per_stat_mae": {k: {m: weighted(lambda p, k=k, m=m: p["per_stat_mae"][k][m]) for m in ("blend", "live")}
                             for k in parts[0]["per_stat_mae"]},
            "by_season": {str(f["season"]): {m: f["by_days"][d]["fpts_pg_mae"][m] for m in ("blend", "live")}
                          for f in folds if d in f["by_days"]},
        }
        for key in parts[0]:
            if key.startswith(("board_", "games_mae_")):
                entry[key] = round(_mean([p[key] for p in parts]), 4)
        out[str(d)] = entry
    return out
