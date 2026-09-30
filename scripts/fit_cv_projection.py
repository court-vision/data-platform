"""
Fit the Court Vision projection's coefficients and write them, with their
backtest, to static/cv_projection_coefficients.json.

    python scripts/fit_cv_projection.py                  # from nba.player_history
    python scripts/fit_cv_projection.py --from-csv DIR   # from nba_api CSVs (<season>_Base.csv + all_players.csv)

Run once each offseason, after the season-history pipeline has appended the
season just finished. The file is committed: the projection is reproducible
from the repo, and a refit shows up as a reviewable diff.

Two fits are made. The production one uses every season pair available. The
backtest one stops four seasons short, projects those four, and scores them —
so the numbers in the file measure the method on seasons it never saw.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from services.projection_fit import backtest, fit_curve, fit_games  # noqa: E402
from services.projection_model import Coefficients, HistoryRow, season_label  # noqa: E402

OUT = ROOT / "static" / "cv_projection_coefficients.json"
WEIGHTS = (7.0, 2.0, 1.0)
GP_SHRINK = 0.5
ESPN_WEIGHT = 0.5
BACKTEST_SEASONS = 4
FIRST_GAMES_TARGET = 2015

CSV_COLUMNS = {"pts": "PTS", "reb": "REB", "ast": "AST", "stl": "STL", "blk": "BLK", "tov": "TOV",
               "fgm": "FGM", "fga": "FGA", "fg3m": "FG3M", "fg3a": "FG3A", "ftm": "FTM", "fta": "FTA",
               "oreb": "OREB", "dreb": "DREB", "dd2": "DD2", "td3": "TD3"}


def rows_from_csv(directory: str) -> list[HistoryRow]:
    from_year: dict[int, int] = {}
    with open(os.path.join(directory, "all_players.csv")) as f:
        for r in csv.DictReader(f):
            try:
                from_year[int(r["PERSON_ID"])] = int(r["FROM_YEAR"])
            except (KeyError, ValueError):
                continue
    rows: list[HistoryRow] = []
    for path in sorted(glob.glob(os.path.join(directory, "*_Base.csv"))):
        start = int(os.path.basename(path)[:4])
        with open(path) as f:
            for r in csv.DictReader(f):
                pid = int(r["PLAYER_ID"])
                rows.append(HistoryRow(
                    player_id=pid, season=start, gp=int(r["GP"]), minutes=float(r["MIN"]),
                    totals={k: float(r[v] or 0) for k, v in CSV_COLUMNS.items()},
                    age=float(r["AGE"]) if r.get("AGE") else None, from_year=from_year.get(pid),
                ))
    return rows


def rows_from_db() -> list[HistoryRow]:
    from db.models.nba import PlayerHistory
    from services.projection_model import season_start

    return [
        HistoryRow(
            player_id=r.player_id, season=season_start(r.season), gp=int(r.gp), minutes=float(r.min),
            totals={k: float(getattr(r, k) or 0) for k in PlayerHistory.TOTAL_KEYS},
            age=float(r.age) if r.age is not None else None, from_year=r.from_year,
        )
        for r in PlayerHistory.select()
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--from-csv", help="directory of nba_api LeagueDashPlayerStats CSVs")
    parser.add_argument("--out", default=str(OUT))
    args = parser.parse_args()

    rows = rows_from_csv(args.from_csv) if args.from_csv else rows_from_db()
    seasons = sorted({r.season for r in rows})
    if len(seasons) < BACKTEST_SEASONS + 4:
        print(f"need at least {BACKTEST_SEASONS + 4} seasons of history, have {len(seasons)}", file=sys.stderr)
        return 1
    last = seasons[-1]

    held_out = list(range(last - BACKTEST_SEASONS + 1, last + 1))
    bt_curve = fit_curve(rows, through=held_out[0] - 1)
    bt_beta = fit_games(rows, through=held_out[0] - 1, first=FIRST_GAMES_TARGET)
    bt = backtest(rows, held_out, Coefficients(curve=bt_curve, gp_beta=bt_beta, weights=WEIGHTS, gp_shrink=GP_SHRINK))
    # The baseline is last season as it happened: its per-game line, its games.
    last_games_only = [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    naive = backtest(rows, held_out, Coefficients(
        curve={}, gp_beta=last_games_only, weights=(1.0, 0.0, 0.0), reg_minutes=0.0, reg_games=0.0, gp_shrink=0.0,
    ))

    curve = fit_curve(rows, through=last)
    beta = fit_games(rows, through=last, first=FIRST_GAMES_TARGET)
    version = f"cv-{season_label(last + 1)}-{date.today().isoformat()}"
    doc = {
        "version": version,
        "fitted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fitted_on": {"seasons": [season_label(seasons[0]), season_label(last)], "rows": len(rows),
                      "source": "csv" if args.from_csv else "nba.player_history"},
        "weights": list(WEIGHTS),
        "reg_minutes": 600.0,
        "reg_games": 20.0,
        "gp_mean": 0.72,
        "gp_shrink": GP_SHRINK,
        "espn_weight": ESPN_WEIGHT,
        "gp_beta": [round(b, 6) for b in beta],
        "backtest": {
            "held_out": [season_label(s) for s in held_out],
            "model": {"fpts_pg_mae": bt.fpts_pg_mae, "games_mae": bt.games_mae,
                      "points_rho_top200": bt.points_rho_top200},
            "last_season_line": {"fpts_pg_mae": naive.fpts_pg_mae, "games_mae": naive.games_mae,
                                 "points_rho_top200": naive.points_rho_top200},
        },
        "curve": {k: {s: round(v, 6) for s, v in sorted(c.items())} for k, c in sorted(curve.items())},
    }
    Path(args.out).write_text(json.dumps(doc, indent=1) + "\n")
    print(json.dumps({"version": version, "backtest": doc["backtest"]}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
