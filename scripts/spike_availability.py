"""
Availability spike (docs/NEWS_AND_INJURY_PIPELINES_PLAN.md § 6). Read-only probes.

Three questions the availability pipeline's due rules depend on, none of which
can be answered from the dev machine (cdn.nba.com and ak-static.cms.nba.com
answer it 403). Run this where prod data-platform runs, e.g. `railway ssh` into
the data-platform service, under nohup so it survives the session:

    nohup python scripts/spike_availability.py boxscore --out /tmp/spike > /tmp/spike/boxscore.log 2>&1 &
    nohup python scripts/spike_availability.py report --date 2026-10-19 --out /tmp/spike > /tmp/spike/report.log 2>&1 &
    python scripts/spike_availability.py espn-etag

It reads nothing from and writes nothing to the database. Every observation is
also printed as one JSON line, so the Railway log keeps the result even if the
container's /tmp does not survive.

boxscore
    For each of today's games (ET date, from the league schedule feed), fetch
    `boxscore_{gameId}.json` every --interval seconds from --lead minutes before
    tip until --tail minutes after it. One CSV row per fetch: HTTP status, game
    status, player count, how many players are INACTIVE, the distinct
    `notPlayingReason` values and how many starters are flagged. The first
    response that lists any INACTIVE player is saved whole as a fixture. The
    answer we want: minutes-before-tip at which the inactive list first appears
    (sets the pre-tip polling window in § 2).

report
    For --date, from 7 AM ET (4 PM the day before with --include-eve) until the
    end of the day (or --until), HEAD-probe every candidate filename of the NBA official injury report for
    the current and previous quarter hour, every --interval seconds. Each name
    that answers 200 is logged once with its first-seen time and downloaded to
    --out. The answer we want: which filename shape the league uses this season
    and how often a new snapshot appears; the PDFs become parser fixtures.

espn-etag
    One GET (then one conditional GET) of the ESPN injuries JSON; prints the
    caching headers and whether a conditional request is answered 304.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Ensure data-platform/ is on sys.path regardless of where the script is invoked from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests

from core.season import season_for_date
from utils.nba_cdn import fetch_league_schedule, headers_for_url, load_static_schedule

ET = ZoneInfo("America/New_York")
BOXSCORE_URL = "https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
REPORT_URL = "https://ak-static.cms.nba.com/referee/injury/Injury-Report_{name}.pdf"
ESPN_INJURIES_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def emit(kind: str, **fields) -> None:
    print(json.dumps({"spike": kind, "at": now_utc().isoformat(timespec="seconds"), **fields}), flush=True)


def proxies() -> dict | None:
    """The residential proxy prod uses for nba_api, when configured."""
    try:
        from core.settings import settings

        url = settings.nba_api_proxy_url
    except Exception:
        url = os.environ.get("NBA_API_PROXY_URL")
    return {"http": url, "https": url} if url else None


# --- boxscore ---------------------------------------------------------------


def todays_games(day: date) -> list[dict]:
    """[{game_id, tip (UTC datetime), matchup}] for the ET date `day`."""
    season = season_for_date(day)
    try:
        feed = fetch_league_schedule(season)
        source = "cdn"
    except Exception as exc:  # the checked-in copy has the same shape
        emit("schedule_fallback", error=str(exc))
        feed = load_static_schedule(season)
        source = "static"
    games = []
    for game_date in feed["leagueSchedule"]["gameDates"]:
        for g in game_date["games"]:
            tip = datetime.fromisoformat(g["gameDateTimeUTC"].replace("Z", "+00:00"))
            if tip.astimezone(ET).date() != day:
                continue
            games.append(
                {
                    "game_id": g["gameId"],
                    "tip": tip,
                    "matchup": f"{g['awayTeam']['teamTricode']}@{g['homeTeam']['teamTricode']}",
                }
            )
    emit("schedule", source=source, day=day.isoformat(), games=len(games))
    return sorted(games, key=lambda g: g["tip"])


def summarize_boxscore(payload: dict) -> dict:
    game = payload.get("game") or {}
    players = [
        p for side in ("homeTeam", "awayTeam") for p in (game.get(side) or {}).get("players") or []
    ]
    inactive = [p for p in players if p.get("status") == "INACTIVE"]
    return {
        "game_status": game.get("gameStatus"),
        "players": len(players),
        "inactive": len(inactive),
        "starters": sum(1 for p in players if str(p.get("starter")) == "1"),
        "reasons": sorted({p.get("notPlayingReason") or "" for p in inactive} - {""}),
        "inactive_names": sorted(p.get("nameI") or p.get("name") or "" for p in inactive),
    }


def run_boxscore(args) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    day = date.fromisoformat(args.date) if args.date else now_utc().astimezone(ET).date()
    games = todays_games(day)
    if not games:
        emit("done", reason="no games")
        return
    csv_path = out / f"boxscore_{day.isoformat()}.csv"
    new_file = not csv_path.exists()
    fh = open(csv_path, "a", newline="")
    writer = csv.writer(fh)
    if new_file:
        writer.writerow(
            ["observed_at", "game_id", "matchup", "tip_utc", "min_to_tip", "http",
             "game_status", "players", "inactive", "starters", "reasons"]
        )
    first_inactive: dict[str, float] = {}
    first_starters: dict[str, float] = {}
    session = requests.Session()
    lead, tail = timedelta(minutes=args.lead), timedelta(minutes=args.tail)

    while True:
        now = now_utc()
        pending = [g for g in games if now <= g["tip"] + tail]
        if not pending:
            break
        for g in pending:
            if now < g["tip"] - lead:
                continue
            gid = g["game_id"]
            url = BOXSCORE_URL.format(game_id=gid)
            min_to_tip = round((g["tip"] - now).total_seconds() / 60, 1)
            summary: dict = {}
            try:
                resp = session.get(url, headers=headers_for_url(url), proxies=proxies(), timeout=20)
                status = resp.status_code
                if status == 200:
                    payload = resp.json()
                    summary = summarize_boxscore(payload)
                    if summary["inactive"] and gid not in first_inactive:
                        first_inactive[gid] = min_to_tip
                        (out / f"boxscore_{gid}_first_inactive.json").write_text(json.dumps(payload))
                        emit("first_inactive", game_id=gid, matchup=g["matchup"],
                             min_to_tip=min_to_tip, **summary)
                    if summary["starters"] and gid not in first_starters:
                        first_starters[gid] = min_to_tip
                        emit("first_starters", game_id=gid, matchup=g["matchup"],
                             min_to_tip=min_to_tip, starters=summary["starters"])
            except Exception as exc:
                status = f"error:{type(exc).__name__}"
            writer.writerow(
                [now.isoformat(timespec="seconds"), gid, g["matchup"], g["tip"].isoformat(),
                 min_to_tip, status, summary.get("game_status"), summary.get("players"),
                 summary.get("inactive"), summary.get("starters"), "|".join(summary.get("reasons", []))]
            )
            emit("boxscore", game_id=gid, min_to_tip=min_to_tip, http=status,
                 **{k: v for k, v in summary.items() if k != "inactive_names"})
        fh.flush()
        # Sleep to the next poll, or to the next game's window if none is open yet.
        now = now_utc()
        next_open = min((g["tip"] - lead for g in games if g["tip"] - lead > now), default=now)
        open_now = any(g["tip"] - lead <= now <= g["tip"] + tail for g in games)
        time.sleep(args.interval if open_now else max(args.interval, (next_open - now).total_seconds()))

    fh.close()
    emit("done", day=day.isoformat(),
         first_inactive_min_to_tip=first_inactive, first_starters_min_to_tip=first_starters,
         games_without_inactive=[g["game_id"] for g in games if g["game_id"] not in first_inactive])


# --- report -----------------------------------------------------------------


def report_names(at_et: datetime) -> list[str]:
    """Every filename shape seen or plausible for the quarter hour containing `at_et`.

    The file is named for when it was published (ET), not for the game date: the
    5 PM report on D-1 is `Injury-Report_{D-1}_05PM.pdf` and lists D's games.
    """
    hour12 = at_et.strftime("%I")
    ampm = at_et.strftime("%p")
    minute = (at_et.minute // 15) * 15
    stem = at_et.date().isoformat()
    names = [f"{stem}_{hour12}_{minute:02d}{ampm}"]
    if minute == 0:
        names += [f"{stem}_{hour12}{ampm}", f"{stem}_{hour12}_00{ampm}"]
    return list(dict.fromkeys(names))


def run_report(args) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    day = date.fromisoformat(args.date)
    start = datetime.combine(day, datetime.min.time(), ET).replace(hour=7)
    # The day-before 5 PM report lists --date's games, so optionally start D-1 evening.
    if args.include_eve:
        start = start - timedelta(hours=15)  # 4 PM ET the day before
    until = (
        datetime.fromisoformat(args.until).replace(tzinfo=ET)
        if args.until
        else datetime.combine(day, datetime.max.time(), ET)
    )
    seen: dict[str, str] = {}
    session = requests.Session()
    headers = {"User-Agent": BROWSER_UA, "Accept": "application/pdf,*/*"}
    while now_utc().astimezone(ET) <= until:
        now_et = now_utc().astimezone(ET)
        if now_et < start:
            time.sleep(min(600, (start - now_et).total_seconds()))
            continue
        candidates = report_names(now_et) + report_names(now_et - timedelta(minutes=15))
        for name in dict.fromkeys(candidates):
            if name in seen:
                continue
            url = REPORT_URL.format(name=name)
            try:
                resp = session.head(url, headers=headers, timeout=15, allow_redirects=True)
                status = resp.status_code
            except Exception as exc:
                status = f"error:{type(exc).__name__}"
            if status == 200:
                seen[name] = now_et.isoformat(timespec="seconds")
                pdf = session.get(url, headers=headers, timeout=30)
                (out / f"Injury-Report_{name}.pdf").write_bytes(pdf.content)
                emit("report_found", name=name, first_seen_et=seen[name], bytes=len(pdf.content),
                     last_modified=resp.headers.get("Last-Modified"))
            else:
                emit("report_probe", name=name, http=status)
        time.sleep(args.interval)
    emit("done", day=day.isoformat(), found=seen)


# --- espn-etag --------------------------------------------------------------


def run_espn_etag(args) -> None:
    headers = {"User-Agent": BROWSER_UA, "Accept-Encoding": "gzip"}
    first = requests.get(ESPN_INJURIES_URL, headers=headers, timeout=30)
    cache = {k: first.headers.get(k) for k in ("ETag", "Last-Modified", "Cache-Control", "Age", "Expires")}
    emit("espn_first", http=first.status_code, bytes=len(first.content), **cache)
    conditional = dict(headers)
    if cache["ETag"]:
        conditional["If-None-Match"] = cache["ETag"]
    if cache["Last-Modified"]:
        conditional["If-Modified-Since"] = cache["Last-Modified"]
    if len(conditional) == len(headers):
        emit("espn_conditional", skipped="no validators returned")
        return
    second = requests.get(ESPN_INJURIES_URL, headers=conditional, timeout=30)
    emit("espn_conditional", http=second.status_code, bytes=len(second.content))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    box = sub.add_parser("boxscore")
    box.add_argument("--date", help="ET game date (default: today ET)")
    box.add_argument("--lead", type=int, default=120, help="minutes before tip to start polling")
    box.add_argument("--tail", type=int, default=10, help="minutes after tip to keep polling")
    box.add_argument("--interval", type=int, default=120, help="seconds between polls")
    box.add_argument("--out", default="/tmp/spike")
    box.set_defaults(func=run_boxscore)

    rep = sub.add_parser("report")
    rep.add_argument("--date", required=True, help="game date the report is named for")
    rep.add_argument("--include-eve", action="store_true", help="also probe from 4 PM ET the day before")
    rep.add_argument("--until", help="stop at this ET time (YYYY-MM-DDTHH:MM)")
    rep.add_argument("--interval", type=int, default=300, help="seconds between probe rounds")
    rep.add_argument("--out", default="/tmp/spike")
    rep.set_defaults(func=run_report)

    etag = sub.add_parser("espn-etag")
    etag.set_defaults(func=run_espn_etag)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
