"""
Fitting and backtesting the Court Vision projection (services.projection_model).

Pure: history rows in, coefficients or metrics out. `scripts/fit_cv_projection.py`
is the entry point; it reads nba.player_history (or a directory of nba_api CSVs)
and writes `static/cv_projection_coefficients.json`.

**The curve** is the delta method: for every player with 300+ minutes in two
consecutive seasons, the log change of each per-minute rate, weighted by the
harmonic mean of the two seasons' minutes, averaged by the bucket of the later
season. Before averaging, the league's own change that year is subtracted —
without it the three-point boom reads as every player of every age learning to
shoot, and the projection carries a decade of league drift into next season.
Age buckets are then smoothed across neighbours; experience buckets are not.

**Games** are a least-squares fit of each season's games fraction on the three
before it and age. A player active before and after a season with no row in it
missed that whole season, and counts as 0 — the case a naive fit drops, and
exactly the injured-star case the projection must see.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Sequence

from services.projection_model import (
    RATE_KEYS,
    SEASON_GAMES,
    Coefficients,
    HistoryRow,
    bucket,
    cell_means,
    games_fraction,
    role_cells,
    season_games,
    statistical_line,
)

MIN_PAIR_MINUTES = 300.0
SMOOTH_AGES = range(22, 38)


def by_player(rows: Iterable[HistoryRow]) -> dict[int, dict[int, HistoryRow]]:
    out: dict[int, dict[int, HistoryRow]] = defaultdict(dict)
    for r in rows:
        out[r.player_id][r.season] = r
    return out


def league_drift(rows: Iterable[HistoryRow]) -> dict[int, dict[str, float]]:
    """Season -> log change of the league's per-minute rate of each stat from the season before."""
    sums: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in rows:
        for k in RATE_KEYS:
            sums[r.season][k] += r.totals.get(k, 0.0)
        sums[r.season]["min"] += r.minutes
    rates = {s: {k: v[k] / v["min"] for k in RATE_KEYS} for s, v in sums.items() if v["min"] > 0}
    return {
        s: {k: math.log((rates[s][k] + 1e-9) / (rates[s - 1][k] + 1e-9)) for k in RATE_KEYS}
        for s in rates if s - 1 in rates
    }


def fit_curve(
    rows: Sequence[HistoryRow], through: int, detrend: bool = True
) -> dict[str, dict[str, float]]:
    """The aging/experience curve from season pairs whose later season is <= `through`."""
    players = by_player(rows)
    drift = league_drift(rows) if detrend else {}
    acc: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for seasons in players.values():
        for season, cur in seasons.items():
            prev = seasons.get(season - 1)
            if season > through or prev is None:
                continue
            if cur.minutes < MIN_PAIR_MINUTES or prev.minutes < MIN_PAIR_MINUTES:
                continue
            w = 2.0 / (1.0 / prev.minutes + 1.0 / cur.minutes)
            key = bucket(cur.exp_in(season), cur.age)
            a = acc[key]
            a["w"] += w
            a["n"] += 1
            a["MPG"] += w * (cur.mpg - prev.mpg)
            for k in RATE_KEYS:
                before = prev.totals.get(k, 0.0) / prev.minutes
                after = cur.totals.get(k, 0.0) / cur.minutes
                change = math.log((after + 1e-4) / (before + 1e-4)) - drift.get(season, {}).get(k, 0.0)
                a[k] += w * change
    raw = {
        key: {k: math.exp(a[k] / a["w"]) for k in RATE_KEYS} | {"MPG": a["MPG"] / a["w"], "n": a["n"]}
        for key, a in acc.items() if a["w"] > 0
    }
    return _smooth(raw)


def _smooth(curve: Mapping[str, Mapping[str, float]]) -> dict[str, dict[str, float]]:
    """3-point, count-weighted smoothing across neighbouring age buckets."""
    out = {k: dict(v) for k, v in curve.items()}
    ages = [a for a in SMOOTH_AGES if f"age{a}" in curve]
    for i, a in enumerate(ages):
        near = [x for x in (ages[i - 1] if i else None, a, ages[i + 1] if i + 1 < len(ages) else None) if x is not None]
        ns = [curve[f"age{x}"]["n"] for x in near]
        total = sum(ns)
        for k in RATE_KEYS:
            out[f"age{a}"][k] = math.exp(sum(n * math.log(curve[f"age{x}"][k]) for x, n in zip(near, ns)) / total)
        out[f"age{a}"]["MPG"] = sum(n * curve[f"age{x}"]["MPG"] for x, n in zip(near, ns)) / total
    return out


def _games_features(prior: Mapping[int, float], age: Optional[float]) -> list[float]:
    f = [1.0]
    for lag in (1, 2, 3):
        v = prior.get(lag)
        f += [0.0 if v is None else v, 1.0 if v is None else 0.0]
    a = age if age is not None else 27.0
    return f + [max(a - 28.0, 0.0), max(24.0 - a, 0.0)]


def fit_games(rows: Sequence[HistoryRow], through: int, first: int) -> list[float]:
    """Least-squares games model on target seasons first..through."""
    players = by_player(rows)
    X: list[list[float]] = []
    y: list[float] = []
    for seasons in players.values():
        last_seen = max(seasons)
        for t in range(first, through + 1):
            prev = seasons.get(t - 1)
            if prev is None or prev.minutes < MIN_PAIR_MINUTES:
                continue
            if t in seasons:
                target = seasons[t].games_fraction
            elif last_seen > t:
                target = 0.0                        # missed the whole season, came back
            else:
                continue                            # left the league
            prior = {lag: seasons[t - lag].games_fraction for lag in (1, 2, 3) if (t - lag) in seasons}
            age = (prev.age + 1.0) if prev.age is not None else None
            X.append(_games_features(prior, age))
            y.append(target)
    return _least_squares(X, y)


def _least_squares(X: list[list[float]], y: list[float]) -> list[float]:
    """Normal equations with a tiny ridge, solved by Gaussian elimination."""
    n = len(X[0])
    A = [[sum(r[i] * r[j] for r in X) + (1e-9 if i == j else 0.0) for j in range(n)] for i in range(n)]
    b = [sum(r[i] * t for r, t in zip(X, y)) for i in range(n)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(A[r][col]))
        A[col], A[pivot] = A[pivot], A[col]
        b[col], b[pivot] = b[pivot], b[col]
        for r in range(n):
            if r != col and A[col][col]:
                f = A[r][col] / A[col][col]
                for c in range(col, n):
                    A[r][c] -= f * A[col][c]
                b[r] -= f * b[col]
    return [b[i] / A[i][i] if A[i][i] else 0.0 for i in range(n)]


# ---- backtest -------------------------------------------------------------------------


POINTS = {"pts": 1, "reb": 1, "ast": 2, "stl": 4, "blk": 4, "tov": -2, "fg3m": 1,
          "fgm": 2, "fga": -1, "ftm": 1, "fta": -1}


def _fpts(line: Mapping[str, float]) -> float:
    return sum(w * line.get(k, 0.0) for k, w in POINTS.items())


def _spearman(a: Sequence[float], b: Sequence[float]) -> float:
    def ranks(v: Sequence[float]) -> list[float]:
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0.0] * len(v)
        i = 0
        while i < len(order):
            j = i
            while j + 1 < len(order) and v[order[j + 1]] == v[order[i]]:
                j += 1
            for k in range(i, j + 1):
                r[order[k]] = (i + j) / 2.0
            i = j + 1
        return r

    ra, rb = ranks(a), ranks(b)
    n = len(a)
    ma, mb = sum(ra) / n, sum(rb) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    va = math.sqrt(sum((x - ma) ** 2 for x in ra))
    vb = math.sqrt(sum((y - mb) ** 2 for y in rb))
    return cov / (va * vb) if va and vb else 0.0


@dataclass(frozen=True)
class BacktestResult:
    fpts_pg_mae: float
    games_mae: float
    points_rho_top200: float
    targets: tuple[int, ...]


def backtest(rows: Sequence[HistoryRow], targets: Sequence[int], coeffs: Coefficients) -> BacktestResult:
    """Project each target season from the three before it; score against what happened.

    Per-game error over players with 20+ games in the target; games error and
    points-season rank correlation (top 200 by actual season points) over every
    player active in the target, whole missed seasons included as 0.
    """
    players = by_player(rows)
    pg_err: list[float] = []
    gp_err: list[float] = []
    rhos: list[float] = []
    for t in targets:
        window = {pid: [s[y] for y in (t - 1, t - 2, t - 3) if y in s and s[y].minutes > 0]
                  for pid, s in players.items()}
        window = {pid: rs for pid, rs in window.items() if rs}
        cells = role_cells(window)
        by_cell, league = cell_means(window, cells, t - 1)
        proj_v: list[float] = []
        act_v: list[float] = []
        for pid, rs in window.items():
            seasons = players[pid]
            if t in seasons:
                actual = seasons[t]
            elif max(seasons) > t:
                actual = None                         # missed all of t
            else:
                continue
            p = statistical_line(rs, t, coeffs, by_cell.get(cells.get(pid), league))
            if p is None:
                continue
            actual_fraction = actual.games_fraction if actual else 0.0
            gp_err.append(abs(p.games / SEASON_GAMES - actual_fraction))
            proj_pg = _fpts(p.per_game)
            if actual and actual.gp >= 20:
                pg_err.append(abs(proj_pg - _fpts({k: v / actual.gp for k, v in actual.totals.items()})))
            proj_v.append(proj_pg * (p.games / SEASON_GAMES) * season_games(t))
            act_v.append(_fpts(actual.totals) if actual else 0.0)
        top = sorted(range(len(act_v)), key=lambda i: -act_v[i])[:200]
        rhos.append(_spearman([proj_v[i] for i in top], [act_v[i] for i in top]))
    return BacktestResult(
        fpts_pg_mae=round(sum(pg_err) / len(pg_err), 3),
        games_mae=round(sum(gp_err) / len(gp_err), 4),
        points_rho_top200=round(sum(rhos) / len(rhos), 4),
        targets=tuple(targets),
    )


def games_fraction_for(rows: Sequence[HistoryRow], target: int, coeffs: Coefficients) -> float:
    """Convenience for scripts: one player's expected games fraction."""
    latest = max(rows, key=lambda r: r.season)
    age = (latest.age + (target - latest.season)) if latest.age is not None else None
    return games_fraction(rows, target, age, coeffs)
