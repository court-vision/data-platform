"""
Fit the live projection's coefficients and write them, with their backtest, to
static/live_projection_coefficients.json.

    python scripts/fit_live_projection.py                  # from nba.player_game_stats and nba.player_history
    python scripts/fit_live_projection.py --from-csv DIR   # from CSVs (pgs_<season>.csv + player_history.csv)

Run once each offseason, after the season just finished is whole in
nba.player_game_stats. The file is committed: the projection is reproducible
from the repo, and a refit shows up as a reviewable diff.

Two things are made. The production fit uses every past season. The backtest
refits on all of them but one and scores that one, for each season in turn, so
the numbers in the file measure the method on seasons it never saw
(services/live_projection_fit.py says what is fitted and how it is scored).

A season is used when the two before it are there (its preseason projection is
built from them), it is finished, and it is not 2020-21, whose December start
puts every date in the wrong place.

The CSV directory holds one `pgs_<season>.csv` per season (player_id, team_id,
game_id, game_date, min, the box score columns, oreb, dreb: regular season
only) and `player_history.csv` (player_id, season, age, from_year).
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from datetime import date, datetime, timezone
from multiprocessing import Pool
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services import live_projection_fit as fitting  # noqa: E402
from services.live_projection_model import LINE_KEYS, BoxLine  # noqa: E402
from services.projection_model import Coefficients as SeasonCoefficients  # noqa: E402
from services.projection_model import season_label, season_start  # noqa: E402

OUT = ROOT / "static" / "live_projection_coefficients.json"
SEASON_COEFFICIENTS = ROOT / "static" / "cv_projection_coefficients.json"
SKIPPED_SEASONS = (2020,)
STAT_COLUMNS = LINE_KEYS + ("oreb", "dreb")

_BY_SEASON: dict = {}        # the samples, shared with the backtest's worker processes


def lines_from_csv(directory: str) -> tuple[dict[int, list[BoxLine]], dict]:
    lines: dict[int, list[BoxLine]] = {}
    for path in sorted(glob.glob(os.path.join(directory, "pgs_*.csv"))):
        season = int(os.path.basename(path)[4:8])
        with open(path) as f:
            lines[season] = [
                BoxLine(
                    player_id=int(r["player_id"]), team=r["team_id"], game_id=r["game_id"],
                    game_date=date.fromisoformat(r["game_date"][:10]), minutes=float(r["min"] or 0),
                    stats={k: float(r.get(k) or 0) for k in STAT_COLUMNS},
                )
                for r in csv.DictReader(f) if r.get("team_id") and r.get("game_id")
            ]
    bio: dict = {}
    with open(os.path.join(directory, "player_history.csv")) as f:
        for r in csv.DictReader(f):
            bio[(int(r["player_id"]), season_start(r["season"]))] = (
                float(r["age"]) if r.get("age") else None,
                int(float(r["from_year"])) if r.get("from_year") else None,
            )
    return lines, bio


def lines_from_db() -> tuple[dict[int, list[BoxLine]], dict]:
    from db.models.nba import PlayerHistory
    from db.models.nba.player_game_stats import PlayerGameStats as G

    columns = [getattr(G, k) for k in STAT_COLUMNS]
    query = (
        G.select(G.player, G.team, G.game, G.game_date, G.season, G.min, *columns)
        .where((G.season_type == "regular") & G.game.is_null(False) & G.team.is_null(False))
        .tuples()
    )
    lines: dict[int, list[BoxLine]] = {}
    for pid, team, game_id, day, season, minutes, *stats in query.iterator():
        lines.setdefault(season_start(season), []).append(BoxLine(
            player_id=pid, team=team, game_id=game_id, game_date=day, minutes=float(minutes or 0),
            stats={k: float(v or 0) for k, v in zip(STAT_COLUMNS, stats)},
        ))
    bio = {
        (r.player_id, season_start(r.season)): (float(r.age) if r.age is not None else None, r.from_year)
        for r in PlayerHistory.select(PlayerHistory.player_id, PlayerHistory.season, PlayerHistory.age,
                                      PlayerHistory.from_year)
    }
    return lines, bio


def usable(lines: dict[int, list[BoxLine]]) -> list[int]:
    out = []
    for season, rows in sorted(lines.items()):
        if season in SKIPPED_SEASONS or season - 1 not in lines or season - 2 not in lines:
            continue
        if max(l.game_date for l in rows) < date(season + 1, 3, 1):
            continue                    # still being played
        out.append(season)
    return out


def _fold(season: int) -> dict:
    return fitting.hold_out(_BY_SEASON, season)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--from-csv", help="directory of box score CSVs")
    parser.add_argument("--out", default=str(OUT))
    parser.add_argument("--jobs", type=int, default=4, help="processes for the backtest's folds")
    parser.add_argument("--no-backtest", action="store_true", help="fit only (the file then carries no backtest)")
    args = parser.parse_args()

    lines, bio = lines_from_csv(args.from_csv) if args.from_csv else lines_from_db()
    seasons = usable(lines)
    if len(seasons) < 3:
        print(f"need at least 3 usable seasons, have {seasons}", file=sys.stderr)
        return 1
    season_coeffs = SeasonCoefficients.from_json(json.loads(SEASON_COEFFICIENTS.read_text()))
    history = fitting.history_rows(lines, bio)

    mornings = 0
    for season in seasons:
        priors = fitting.season_priors(history, season, season_coeffs)
        ages = fitting.ages_for(history, season)
        ranked = fitting.last_season_ranks(history, season)
        sat_out = fitting.sat_out_season(lines, season)
        _BY_SEASON[season] = [
            fitting.build_sample(season, days, lines[season], priors, ages, ranked, sat_out)
            for days in fitting.CUTOFFS
        ]
        mornings += len(_BY_SEASON[season])
        print(f"{season_label(season)}: {len(lines[season])} lines, {len(priors)} projected, "
              f"{len(sat_out)} sat the season out", flush=True)

    backtest = None
    if not args.no_backtest:
        with Pool(max(1, args.jobs)) as pool:        # forked: the workers read _BY_SEASON as it stands
            folds = pool.map(_fold, seasons)
        backtest = {
            "method": "each season scored by a fit on the others; the blend is the preseason projection and "
                      "this season's per-game line at ten games' weight, the projection's games share of the games left",
            "seasons": [season_label(s) for s in seasons],
            "scored_on": f"top {fitting.SAMPLE_TOP} by last season's fantasy points a game, "
                         f"{fitting.SAMPLE_GAMES}+ games each side of the morning",
            "by_days_after_opening_night": fitting.combine(folds),
            "fold_coefficients": {season_label(f["season"]): f["coefficients"] for f in folds},
        }

    coeffs = fitting.fit([s for season in seasons for s in _BY_SEASON[season]])
    version = f"live-{season_label(seasons[-1] + 1)}-{date.today().isoformat()}"
    doc = {
        "version": version,
        "fitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fitted_on": {
            "seasons": [season_label(s) for s in seasons], "mornings": mornings,
            "days_after_opening_night": list(fitting.CUTOFFS),
            "source": "csv" if args.from_csv else "nba.player_game_stats",
            "season_projection": season_coeffs.version,
        },
        **{k: v for k, v in coeffs.to_json().items() if k != "version"},
        "backtest": backtest,
    }
    Path(args.out).write_text(json.dumps(doc, indent=1) + "\n")
    print(json.dumps({"version": version, "minutes": doc["minutes"], "usage_slope": doc["usage_slope"],
                      "development": doc["development"], "prior_weights": doc["prior_weights"],
                      "backtest": (backtest or {}).get("by_days_after_opening_night")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
