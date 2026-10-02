"""
The Court Vision projection: a per-game stat line and expected games for every
rostered player, built from three seasons of history, ESPN's projection, and
the curated adjustments (docs/CV_RANKINGS_PLAN.md §2.1).

Pure: no database, no network. `pipelines/cv_projection.py` loads the inputs
and writes the output; `scripts/fit_cv_projection.py` fits the coefficients.

**The statistical line** (Marcel-style, backtested on 14 seasons):

1. *Rates.* Per-minute rates from the last three seasons, recency weights
   7/2/1 by default. Each season is first aged forward to the target season
   through the fitted curve, one year at a time, then the weighted rate is
   regressed toward the player's role cell (a 4x4 grid of bigness —
   offensive boards and blocks per minute — by playmaking, assists per
   minute) with `reg_minutes` of weight.
2. *Minutes.* Games-weighted minutes per game, aged by the curve's minutes
   deltas, regressed with `reg_games` of weight toward the cell's mean.
3. *The curve.* Year-over-year change by career stage: experience buckets
   for the first three seasons (the year-2 leap lives in `exp1`), age buckets
   after. Fitted by the delta method, with the league's own year-over-year
   drift divided out so the three-point boom is not read as players aging
   into better shooters.
4. *Games.* A linear model of next season's games fraction on the last three
   (a whole missed season counts as 0, not as missing) and age, then shrunk
   `gp_shrink` of the way to a typical season. The most accurate games
   estimate is not the best one for ranking: one lost season means "injured
   star" far more often than "fragile player".

**ESPN.** Where ESPN projects a player, minutes, per-minute rates and games
are blended with ESPN's at `espn_weight`. A player with no NBA minutes takes
ESPN's line as it is. ESPN knows about camp news and role changes that no
box score does; the statistical line knows aging and regression.

**Adjustments** apply last, so "32 minutes" means 32 minutes in the final line:
`minutes` rescales the counting stats at constant per-minute production,
`usage` scales the scoring and playmaking stats together (makes with
attempts, so the percentages hold), `rates` scales single stats, `games` sets
games, and `return_date` caps games at the ones his team plays after it.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date
from typing import Iterable, Mapping, Optional, Sequence

# Every per-minute stat the model carries. The projection table stores the
# first twelve plus minutes; oreb/dreb feed the role cells.
RATE_KEYS: tuple[str, ...] = (
    "pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga",
    "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb",
)
LINE_KEYS: tuple[str, ...] = RATE_KEYS[:12]
# What a usage adjustment scales: scoring and playmaking, makes with attempts.
USAGE_KEYS: tuple[str, ...] = ("pts", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "ast", "tov")

SEASON_GAMES = 82
SHORT_SEASONS: dict[int, int] = {2019: 71, 2020: 72}     # COVID seasons, by start year
MIN_MPG, MAX_MPG = 4.0, 38.0
MIN_GAMES_FRACTION, MAX_GAMES_FRACTION = 0.05, 0.98


def season_start(season: str) -> int:
    """'2025-26' -> 2025."""
    return int(str(season)[:4])


def season_label(start: int) -> str:
    """2025 -> '2025-26'."""
    return f"{start}-{str(start + 1)[-2:]}"


def season_games(start: int) -> int:
    return SHORT_SEASONS.get(start, SEASON_GAMES)


def bucket(exp: Optional[float], age: Optional[float]) -> str:
    """The curve bucket for the season a player is moving INTO.

    Experience for the first three seasons (`exp1` = the second season), age
    after — at 20 and in year two, experience is what changes a player.
    """
    if exp is not None and exp <= 3:
        return f"exp{int(max(exp, 1))}"
    a = int(round(age)) if age is not None else 27
    return f"age{min(max(a, 22), 37)}"


@dataclass(frozen=True)
class HistoryRow:
    """One regular season, totals (nba.player_history)."""

    player_id: int
    season: int                          # start year
    gp: int
    minutes: float
    totals: Mapping[str, float]          # RATE_KEYS plus dd2/td3
    age: Optional[float] = None
    from_year: Optional[int] = None

    @property
    def games_fraction(self) -> float:
        return min(self.gp / season_games(self.season), 1.0)

    @property
    def mpg(self) -> float:
        return self.minutes / self.gp if self.gp else 0.0

    def exp_in(self, start: int) -> Optional[int]:
        return (start - self.from_year) if self.from_year is not None else None


@dataclass(frozen=True)
class Coefficients:
    """Everything fitted or chosen, with where it came from."""

    curve: Mapping[str, Mapping[str, float]]         # bucket -> {stat: factor, "MPG": delta}
    gp_beta: Sequence[float]
    weights: Sequence[float] = (7.0, 2.0, 1.0)
    reg_minutes: float = 600.0
    reg_games: float = 20.0
    gp_mean: float = 0.72
    gp_shrink: float = 0.5
    espn_weight: float = 0.5
    # Rookie-season games fraction of top-10 picks, drafts 2012-2025, a whole
    # missed season counted as 0 (picks 1-5: 0.780, picks 6-10: 0.757; nba_api
    # DraftHistory x season totals, measured 2026-10-01). Lottery picks play
    # more than the typical player — 63 games against 59 — and the ones who
    # earn 24+ minutes average 69.
    rookie_gp_mean: float = 0.77
    version: str = "unversioned"

    @classmethod
    def from_json(cls, doc: Mapping) -> "Coefficients":
        return cls(
            curve=doc["curve"],
            gp_beta=tuple(doc["gp_beta"]),
            weights=tuple(doc.get("weights", (7.0, 2.0, 1.0))),
            reg_minutes=float(doc.get("reg_minutes", 600.0)),
            reg_games=float(doc.get("reg_games", 20.0)),
            gp_mean=float(doc.get("gp_mean", 0.72)),
            gp_shrink=float(doc.get("gp_shrink", 0.5)),
            espn_weight=float(doc.get("espn_weight", 0.5)),
            rookie_gp_mean=float(doc.get("rookie_gp_mean", 0.77)),
            version=str(doc.get("version", "unversioned")),
        )


@dataclass(frozen=True)
class EspnLine:
    """ESPN's projection for the target season (nba.player_projections, source espn)."""

    per_game: Mapping[str, float]        # LINE_KEYS
    minutes: Optional[float]
    games: Optional[float]


@dataclass(frozen=True)
class Adjustment:
    """A curated change (nba.projection_adjustments), already resolved to numbers."""

    id: Optional[int] = None
    minutes: Optional[float] = None
    games: Optional[float] = None
    games_available: Optional[float] = None      # team games on/after return_date, when one is set
    usage: Optional[float] = None
    rates: Mapping[str, float] = field(default_factory=dict)


@dataclass
class Projection:
    player_id: int
    minutes: float
    games: float
    per_game: dict[str, float]
    dd_rate: float = 0.0
    td_rate: float = 0.0
    components: dict = field(default_factory=dict)

    def line(self) -> dict[str, float]:
        """The row nba.player_projections stores: LINE_KEYS plus min, per game."""
        out = {k: round(self.per_game.get(k, 0.0), 2) for k in LINE_KEYS}
        out["min"] = round(self.minutes, 2)
        return out


# ---- the statistical line ---------------------------------------------------------------


def _chain(row: HistoryRow, target: int, curve: Mapping) -> tuple[dict[str, float], float]:
    """Multiplicative rate factors and additive MPG delta carrying `row` to `target`."""
    factors = {k: 1.0 for k in RATE_KEYS}
    delta = 0.0
    for year in range(row.season + 1, target + 1):
        exp = row.exp_in(year)
        age = (row.age + (year - row.season)) if row.age is not None else None
        step = curve.get(bucket(exp, age))
        if not step:
            continue
        for k in RATE_KEYS:
            factors[k] *= float(step.get(k, 1.0))
        delta += float(step.get("MPG", 0.0))
    return factors, delta


def role_cells(history: Mapping[int, Sequence[HistoryRow]]) -> dict[int, tuple[int, int]]:
    """Each player's (bigness, playmaking) quartile over his window's totals."""
    bigs: list[tuple[float, int]] = []
    makers: list[tuple[float, int]] = []
    for pid, rows in history.items():
        minutes = sum(r.minutes for r in rows)
        if minutes <= 0:
            continue
        bigs.append(((sum(r.totals.get("oreb", 0) + r.totals.get("blk", 0) for r in rows)) / minutes, pid))
        makers.append((sum(r.totals.get("ast", 0) for r in rows) / minutes, pid))

    def quartiles(pairs: list[tuple[float, int]]) -> dict[int, int]:
        ordered = sorted(pairs)
        n = len(ordered)
        return {pid: min(3, (i * 4) // n) for i, (_v, pid) in enumerate(ordered)} if n else {}

    qb, qp = quartiles(bigs), quartiles(makers)
    return {pid: (qb[pid], qp[pid]) for pid in qb if pid in qp}


def cell_means(
    history: Mapping[int, Sequence[HistoryRow]], cells: Mapping[int, tuple[int, int]], last: int
) -> tuple[dict[tuple[int, int], dict[str, float]], dict[str, float]]:
    """Per-minute rates and MPG of each role cell, and of the league, in season `last`."""
    def empty() -> dict[str, float]:
        return {k: 0.0 for k in RATE_KEYS} | {"min": 0.0, "gp": 0.0}

    sums: dict[tuple[int, int], dict[str, float]] = {}
    total = empty()
    for pid, rows in history.items():
        cell = cells.get(pid)
        for r in rows:
            if r.season != last or r.minutes <= 0:
                continue
            targets = [total] + ([sums.setdefault(cell, empty())] if cell is not None else [])
            for acc in targets:
                for k in RATE_KEYS:
                    acc[k] += r.totals.get(k, 0.0)
                acc["min"] += r.minutes
                acc["gp"] += r.gp

    def rates(s: Mapping[str, float]) -> dict[str, float]:
        m = s["min"] or 1.0
        return {k: s[k] / m for k in RATE_KEYS} | {"MPG": s["min"] / (s["gp"] or 1.0)}

    return {cell: rates(s) for cell, s in sums.items() if s["min"] > 0}, rates(total)


def games_fraction(rows: Sequence[HistoryRow], target: int, age: Optional[float], coeffs: Coefficients) -> float:
    """Expected share of the target season's games, shrunk toward a typical season."""
    by_lag = {target - r.season: r.games_fraction for r in rows}
    features = [1.0]
    for lag in (1, 2, 3):
        v = by_lag.get(lag)
        features += [0.0 if v is None else v, 1.0 if v is None else 0.0]
    a = age if age is not None else 27.0
    features += [max(a - 28.0, 0.0), max(24.0 - a, 0.0)]
    linear = sum(f * b for f, b in zip(features, coeffs.gp_beta))
    linear = min(max(linear, MIN_GAMES_FRACTION), MAX_GAMES_FRACTION)
    return (1.0 - coeffs.gp_shrink) * linear + coeffs.gp_shrink * coeffs.gp_mean


def statistical_line(
    rows: Sequence[HistoryRow],
    target: int,
    coeffs: Coefficients,
    cell_mean: Mapping[str, float],
) -> Optional[Projection]:
    """The history-only projection for one player (rows = his seasons in the window).

    None when the weights give his seasons no evidence at all — only possible
    with a zero weight, as the last-season-only baseline in the backtest uses.
    """
    weights = {1: coeffs.weights[0], 2: coeffs.weights[1], 3: coeffs.weights[2]}
    weighted_min = 0.0
    rate_num = {k: 0.0 for k in RATE_KEYS}
    games_w = 0.0
    mpg_num = 0.0
    for r in rows:
        w = weights.get(target - r.season, 0.0)
        if w <= 0 or r.minutes <= 0 or r.gp <= 0:
            continue
        factors, delta = _chain(r, target, coeffs.curve)
        for k in RATE_KEYS:
            rate_num[k] += w * r.totals.get(k, 0.0) * factors[k]
        weighted_min += w * r.minutes
        games_w += w * r.gp
        mpg_num += w * r.gp * (r.mpg + delta)
    if weighted_min + coeffs.reg_minutes <= 0 or games_w + coeffs.reg_games <= 0:
        return None
    rates = {
        k: (rate_num[k] + coeffs.reg_minutes * cell_mean[k]) / (weighted_min + coeffs.reg_minutes)
        for k in RATE_KEYS
    }
    mpg = (mpg_num + coeffs.reg_games * cell_mean["MPG"]) / (games_w + coeffs.reg_games)
    mpg = min(max(mpg, MIN_MPG), MAX_MPG)

    latest = max(rows, key=lambda r: r.season)
    age = (latest.age + (target - latest.season)) if latest.age is not None else None
    fraction = games_fraction(rows, target, age, coeffs)

    total_w = sum(weights.get(target - r.season, 0.0) * r.gp for r in rows if r.gp > 0)
    dd = sum(weights.get(target - r.season, 0.0) * r.totals.get("dd2", 0.0) for r in rows)
    td = sum(weights.get(target - r.season, 0.0) * r.totals.get("td3", 0.0) for r in rows)
    return Projection(
        player_id=latest.player_id,
        minutes=mpg,
        games=fraction * SEASON_GAMES,
        per_game={k: rates[k] * mpg for k in RATE_KEYS},
        dd_rate=dd / total_w if total_w else 0.0,
        td_rate=td / total_w if total_w else 0.0,
        components={"age": age, "seasons": sorted(r.season for r in rows)},
    )


# ---- ESPN and adjustments ------------------------------------------------------------------


def blend(
    stat: Optional[Projection],
    espn: Optional[EspnLine],
    player_id: int,
    weight: float,
    rookie_games: float = SEASON_GAMES * 0.77,
) -> Optional[Projection]:
    """Combine the statistical line with ESPN's. Either may be missing.

    A player with no NBA minutes takes ESPN's per-game line as it is, but not
    ESPN's games: every veteran's games are pulled toward the model's estimate
    by the blend, and a rookie left at ESPN's 72 would be the most durable
    player on the board for no reason but having no history. His games are
    blended, at the same weight, with `rookie_games` — what lottery picks have
    actually played as rookies (`Coefficients.rookie_gp_mean`), which is more
    than the typical player's season, not less.
    """
    if stat is None and espn is None:
        return None
    if espn is None or not espn.minutes:
        if stat is not None:
            stat.components["espn_weight"] = 0.0
        return stat
    espn_rates = {k: (espn.per_game.get(k, 0.0) / espn.minutes) for k in LINE_KEYS}
    if stat is None:
        games = (weight * espn.games + (1 - weight) * rookie_games) if espn.games else rookie_games
        return Projection(
            player_id=player_id, minutes=espn.minutes, games=games,
            per_game={k: espn.per_game.get(k, 0.0) for k in LINE_KEYS},
            components={"espn_weight": 1.0},
        )
    w = weight
    minutes = w * espn.minutes + (1 - w) * stat.minutes
    stat_rates = {k: stat.per_game[k] / stat.minutes for k in RATE_KEYS}
    rates = {k: w * espn_rates[k] + (1 - w) * stat_rates[k] if k in espn_rates else stat_rates[k] for k in RATE_KEYS}
    games = (w * espn.games + (1 - w) * stat.games) if espn.games else stat.games
    stat.components["espn_weight"] = w
    return Projection(
        player_id=player_id, minutes=minutes, games=games,
        per_game={k: rates[k] * minutes for k in RATE_KEYS},
        dd_rate=stat.dd_rate, td_rate=stat.td_rate,
        components=dict(stat.components),
    )


def adjust(p: Projection, adj: Optional[Adjustment]) -> Projection:
    """Apply one curated adjustment on top of the blended line."""
    if adj is None:
        return p
    per_game = dict(p.per_game)
    minutes = p.minutes
    if adj.minutes is not None and minutes > 0:
        scale = float(adj.minutes) / minutes
        per_game = {k: v * scale for k, v in per_game.items()}
        minutes = float(adj.minutes)
    if adj.usage is not None:
        for k in USAGE_KEYS:
            if k in per_game:
                per_game[k] *= float(adj.usage)
    for k, m in (adj.rates or {}).items():
        if k in per_game:
            per_game[k] *= float(m)
    games = float(adj.games) if adj.games is not None else p.games
    if adj.games_available is not None:
        # A return date caps games at the ones his team has left. With no games
        # target he plays his usual share of them; with one, it is the target,
        # but never more games than remain.
        left = float(adj.games_available)
        cap = left if adj.games is not None else left * min(p.games / SEASON_GAMES, 1.0)
        games = min(games, cap)
    components = dict(p.components)
    components["adjustment_id"] = adj.id
    return Projection(
        player_id=p.player_id, minutes=minutes, games=max(games, 0.0), per_game=per_game,
        dd_rate=p.dd_rate, td_rate=p.td_rate, components=components,
    )


@dataclass
class Breakdown:
    """One player's projection at each stage, for the editor to show side by side."""

    player_id: int
    statistical: Optional[Projection]       # history alone; None for a player with no NBA minutes
    espn: Optional[EspnLine]                # ESPN's line; None where ESPN projects nobody
    blended: Projection                     # the two combined, before any adjustment
    final: Projection                       # with his adjustment applied: what is published


def project_breakdown(
    target: int,
    history: Mapping[int, Sequence[HistoryRow]],
    espn: Mapping[int, EspnLine],
    adjustments: Mapping[int, Adjustment],
    roster: Iterable[int],
    coeffs: Coefficients,
) -> list[Breakdown]:
    """Every rostered player's projection for the season starting `target`,
    with the lines it was built from.

    `history` must hold only the window's seasons (target-3 .. target-1).
    `roster` is who is in the league now: a retired player keeps his history
    but gets no projection.
    """
    window = {pid: [r for r in rows if target - 3 <= r.season <= target - 1 and r.minutes > 0]
              for pid, rows in history.items()}
    window = {pid: rows for pid, rows in window.items() if rows}
    cells = role_cells(window)
    by_cell, league = cell_means(window, cells, target - 1)

    out: list[Breakdown] = []
    for pid in sorted(set(roster)):
        rows = window.get(pid)
        stat = statistical_line(rows, target, coeffs, by_cell.get(cells.get(pid), league)) if rows else None
        # `blend` hands the statistical line back as the blend when ESPN has
        # none, and later stages write to it; the copy is what the editor shows.
        shown = replace(stat, per_game=dict(stat.per_game), components=dict(stat.components)) if stat else None
        blended = blend(stat, espn.get(pid), pid, coeffs.espn_weight, coeffs.rookie_gp_mean * SEASON_GAMES)
        if blended is None:
            continue
        final = adjust(blended, adjustments.get(pid))
        final.components["version"] = coeffs.version
        out.append(Breakdown(player_id=pid, statistical=shown, espn=espn.get(pid), blended=blended, final=final))
    return out


def project(
    target: int,
    history: Mapping[int, Sequence[HistoryRow]],
    espn: Mapping[int, EspnLine],
    adjustments: Mapping[int, Adjustment],
    roster: Iterable[int],
    coeffs: Coefficients,
) -> list[Projection]:
    """Every rostered player's projection for the season starting `target`:
    the published line of `project_breakdown`."""
    return [b.final for b in project_breakdown(target, history, espn, adjustments, roster, coeffs)]


def team_games_after(day: date, per_day: Mapping[date, Sequence[str]], team: Optional[str]) -> Optional[int]:
    """Games `team` plays on or after `day` (the per-day calendar), or None without a team."""
    if not team:
        return None
    return sum(1 for d, teams in per_day.items() if d >= day and team in teams)
