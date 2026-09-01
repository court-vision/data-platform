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

Resolution matters: a poll every N seconds can only bound |Δ| < N when both
fields flip inside one interval — it cannot establish the sign. The first
overnight run (15-min cadence, 2026-08-30) bracketed the flip to 06:49–07:04
UTC (~2 AM CDT, matching last season's hypothesis), which is why the burst
window below defaults to that neighborhood. The decision-relevant claim is
not "Δ <= 0" (unmeasurable by polling) but "no advanced-period/stale-totals
state was ever observed at resolution R": at R = 30s any bad window is
shorter than 30s, which a 15-minute production gate has ~3% odds per night
of even sampling — and the write guard catches that case regardless. That
claim is what the sunset clause in docs/PENDING_PROD_CHECKS.md #4 cites.

Burst mode: --burst-start/--burst-end (HH:MM, UTC) switch the cadence to
--burst-interval inside the window and back to --interval outside it, so an
overnight run stays ~50 polite requests plus ~150 during the hour that
matters.

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
    # Current matchup's totals, and the *previous* matchup's. The previous pair
    # is what keeps a week-rollover night measurable: the instant
    # currentMatchupPeriod advances, the current-matchup columns switch to the
    # new (0-0) matchup and stop saying anything about whether the finished
    # week's totals absorbed its last day — which the 2026-08-31 run
    # demonstrated by flipping to 0.0/0.0 mid-log. The prev columns keep
    # watching the completed matchup across the boundary.
    home_total = away_total = prev_home = prev_away = None
    for matchup in payload.get("schedule") or []:
        mp = matchup.get("matchupPeriodId")
        if mp == matchup_period:
            home_total = (matchup.get("home") or {}).get("totalPoints")
            away_total = (matchup.get("away") or {}).get("totalPoints")
        elif matchup_period is not None and mp == matchup_period - 1:
            prev_home = (matchup.get("home") or {}).get("totalPoints")
            prev_away = (matchup.get("away") or {}).get("totalPoints")
    return {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "latest_scoring_period": period,
        "current_matchup_period": matchup_period,
        "home_total_points": home_total,
        "away_total_points": away_total,
        "prev_home_total_points": prev_home,
        "prev_away_total_points": prev_away,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--league-id", type=int, required=True, help="A PUBLIC ESPN fantasy baseball league id")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--interval", type=int, default=900, help="Seconds between polls (default 15 min)")
    parser.add_argument("--burst-start", default="06:30", help="UTC HH:MM when the high-frequency window opens (default 06:30 = 1:30 AM CDT)")
    parser.add_argument("--burst-end", default="07:45", help="UTC HH:MM when it closes (default 07:45 = 2:45 AM CDT)")
    parser.add_argument("--burst-interval", type=int, default=30, help="Seconds between polls inside the window (default 30)")
    parser.add_argument("--out", default="flb_probe.csv")
    args = parser.parse_args()

    def parse_hhmm(value: str) -> int:
        hours, minutes = value.split(":")
        return int(hours) * 60 + int(minutes)

    burst_start, burst_end = parse_hhmm(args.burst_start), parse_hhmm(args.burst_end)

    def in_burst(now) -> bool:
        minute = now.hour * 60 + now.minute
        if burst_start <= burst_end:
            return burst_start <= minute < burst_end
        return minute >= burst_start or minute < burst_end  # window wraps midnight

    out = Path(args.out)
    fields = [
        "ts_utc", "latest_scoring_period", "current_matchup_period",
        "home_total_points", "away_total_points",
        "prev_home_total_points", "prev_away_total_points",
    ]
    new_file = not out.exists()
    if not new_file:
        header = out.open().readline().strip().split(",")
        if header != fields:
            print(f"note: {out} has the old column set — appended rows carry two extra "
                  "values; start a fresh --out for a clean file")

    print(
        f"polling flb league {args.league_id} every {args.interval}s -> {out}; "
        f"burst {args.burst_interval}s inside {args.burst_start}-{args.burst_end} UTC (Ctrl-C to stop)"
    )
    was_burst = None
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
        burst = in_burst(datetime.now(timezone.utc))
        if burst != was_burst:
            print(f"cadence: every {args.burst_interval if burst else args.interval}s"
                  f" ({'burst window' if burst else 'baseline'})")
            was_burst = burst
        try:
            time.sleep(args.burst_interval if burst else args.interval)
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    sys.exit(main())
