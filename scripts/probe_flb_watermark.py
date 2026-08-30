"""
Measure ESPN's period-vs-totals ordering on fantasy BASEBALL, tonight.

The basketball probe (docs/PENDING_PROD_CHECKS.md #4) cannot answer until
opening night 2026-10-20 — `latestScoringPeriod` is 0 in the preseason. But
MLB is in season *now*, ESPN fantasy baseball runs on the same lm-api-reads
platform with the same `status.latestScoringPeriod` / `totalPoints` fields,
and the ordering of "period advances" vs "totals materialize" is plausibly a
platform property, not a sport property. So: point this at any PUBLIC fantasy
baseball league and let it run overnight.

    .venv/bin/python scripts/probe_flb_watermark.py --league-id 123456 \
        [--year 2026] [--interval 900] [--out flb_probe.csv]

No cookies needed for a public league. Each poll appends one CSV row:
timestamp, latestScoringPeriod, currentMatchupPeriod, and both sides'
totalPoints for the current matchup. Reading the result:

    Δ = t(totals change) − t(latestScoringPeriod change), per overnight flip.
    Δ <= 0 (totals move first or together)  → the watermark pairing holds.
    Δ > 0  (period advances first)          → the write-side guard in
      pipelines/daily_matchup_scores.py is what protects us, and its
      espn_gate_max_attempts budget should comfortably exceed Δ / poll cadence.

Caveat, honestly: this is *evidence*, not proof — a different sport product on
the same platform. The dormant basketball probe stays deployed and confirms on
opening night either way. Ctrl-C to stop; the CSV survives.
"""

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

ENDPOINT = (
    "https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb/seasons/{year}"
    "/segments/0/leagues/{league_id}"
)


def poll(year: int, league_id: int) -> dict:
    resp = requests.get(
        ENDPOINT.format(year=year, league_id=league_id),
        params={"view": ["mSettings", "mMatchup"]},
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    status = payload.get("status", {}) or {}
    period = status.get("latestScoringPeriod")
    matchup_period = status.get("currentMatchupPeriod")
    home_total = away_total = None
    for matchup in payload.get("schedule") or []:
        if matchup.get("matchupPeriodId") == matchup_period:
            home_total = (matchup.get("home") or {}).get("totalPoints")
            away_total = (matchup.get("away") or {}).get("totalPoints")
            break
    return {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "latest_scoring_period": period,
        "current_matchup_period": matchup_period,
        "home_total_points": home_total,
        "away_total_points": away_total,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--league-id", type=int, required=True, help="A PUBLIC ESPN fantasy baseball league id")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--interval", type=int, default=900, help="Seconds between polls (default 15 min)")
    parser.add_argument("--out", default="flb_probe.csv")
    args = parser.parse_args()

    out = Path(args.out)
    fields = ["ts_utc", "latest_scoring_period", "current_matchup_period", "home_total_points", "away_total_points"]
    new_file = not out.exists()

    print(f"polling flb league {args.league_id} every {args.interval}s -> {out} (Ctrl-C to stop)")
    while True:
        try:
            row = poll(args.year, args.league_id)
            with out.open("a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fields)
                if new_file:
                    writer.writeheader()
                    new_file = False
                writer.writerow(row)
            print(f"{row['ts_utc']}  period={row['latest_scoring_period']}  "
                  f"totals={row['home_total_points']}/{row['away_total_points']}")
        except KeyboardInterrupt:
            return 0
        except Exception as exc:  # keep polling through transient failures
            print(f"poll failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    sys.exit(main())
