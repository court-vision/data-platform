"""
The live projection: every player's line for each game his team has left, as of
this morning (the Live rankings plan, step 2).

The preseason projection (services.projection_model) stops on Oct 31. The
stopgap that followed (backend `services.draft_in_season`) blends it with this
season's per-game line at one weight, counts every game the same whoever else
was on the floor, and knows an absence as a flag with no end. This model prices
the rest of the season in three parts that are updated separately:

- **Skill**: what he does per minute. Each stat has its own weight for the
  preseason projection, in minutes for the per-minute volumes and in attempts
  for the three shooting percentages, because a free-throw percentage settles
  far more slowly than a rebound rate. Points are derived from the makes.
- **Role**: his minutes, and his share of the offense, in a given situation.
  Every game this season is filed under how many of the team's regulars'
  minutes were missing that night. His minutes for a game still to come start
  from his recent ones (a half-life in games) and move by the difference
  between the minutes expected to be missing then and the minutes that were
  missing in his games so far, at a slope that is steep for a bench player and
  nearly flat for a 34-minute starter, and that leans on his own games once he
  has enough of them. So a fill-in's line is the big one while the starter is
  expected to be out, and his usual one after.
- **Games**: the chance he plays each remaining game. It is read off what
  regulars in his position did over eight seasons: of those who had just missed
  this many team games (none, one, two, ... or the whole season so far), the
  share who played the team's k-th game from then. So a healthy player is very
  likely to play tonight and a little less likely to play a game in March, and
  a player who is out comes back along the curve of players who were out as
  long. The whole curve is leaned on his projected games and on how many of
  this season's he has played. A known return date replaces the curve, and a
  player ruled out of his team's next game misses it for certain.

The output is a line per remaining game (`LiveProjection.games_ahead`), so any
window can be priced: the rest of the season is the sum of every game left, a
week is the sum of that week's. `timeline` is the same thing run-length
compressed for storage, and `window` prices a stretch of dates from it.

Pure: no database, no network. `pipelines/live_projection.py` loads the inputs
and writes the output; `services/live_projection_fit.py` fits the coefficients
and backtests them, and `scripts/fit_live_projection.py` is its entry point.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Iterable, Mapping, Optional, Sequence

# The stats a line carries, per game (nba.player_projections' columns, minus min)
LINE_KEYS: tuple[str, ...] = (
    "pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
)
# Per-minute volumes, each with its own prior weight (in minutes)
VOLUME_KEYS: tuple[str, ...] = ("reb", "ast", "stl", "blk", "tov", "fga", "fg3a", "fta")
# The three shooting percentages, each with its own prior weight (in attempts)
SHOOTING: dict[str, tuple[str, str]] = {"fg2": ("fg2m", "fg2a"), "fg3": ("fg3m", "fg3a"), "ft": ("ftm", "fta")}
# What a change in his share of the offense scales: shots, free throws, assists, turnovers
USAGE_VOLUMES: tuple[str, ...] = ("fga", "fg3a", "fta", "ast", "tov")
# The line's stats that follow from those
USAGE_LINE_KEYS: tuple[str, ...] = ("pts", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "ast", "tov")

# A percentage from very few attempts starts at the league's (PCT_PAD attempts of it)
LEAGUE_PCT: dict[str, float] = {"fg2": 0.52, "fg3": 0.355, "ft": 0.77}
PCT_PAD = 50.0

# How many team games a player has just missed, as buckets: the return curve is
# fitted per bucket. One more bucket holds players who have not played this season.
STREAK_BUCKETS: tuple[int, ...] = (0, 1, 2, 3, 5, 8, 13, 21)
PRE_DEBUT = len(STREAK_BUCKETS)
BUCKETS = PRE_DEBUT + 1
HORIZON = 70                    # team games ahead the curves are measured over

REGULAR_MINUTES = 20.0          # a "regular": established minutes at or above this
ESTABLISHED_GAMES = 5.0         # the projection's weight in a player's established minutes
MAX_MINUTES = 42.0
SEASON_GAMES = 82.0
MIN_PLAY_SHARE, MAX_PLAY_SHARE = 0.2, 0.98
MAX_PLAY_CHANCE = 0.99
DEVELOPING = (("u22", 22.0), ("u24", 24.0))     # age up to: the in-season development buckets


def bucket(streak: int) -> int:
    """The return-curve bucket for a player who has missed `streak` straight team games."""
    for i in range(len(STREAK_BUCKETS) - 1, -1, -1):
        if streak >= STREAK_BUCKETS[i]:
            return i
    return 0


def usage_of(stats: Mapping[str, float]) -> float:
    """Possessions a line uses: shots, trips to the line and turnovers."""
    return float(stats.get("fga", 0.0)) + 0.44 * float(stats.get("fta", 0.0)) + float(stats.get("tov", 0.0))


@dataclass(frozen=True)
class Coefficients:
    """Everything fitted, with where it came from (static/live_projection_coefficients.json)."""

    plays: Sequence[Sequence[float]]            # [bucket][k]: share who play the k-th team game from now
    played_once_back: Sequence[float]           # [bucket]: share of games played once back, the curve's level
    lean_prior_share: float                     # per unit of the projection's games share
    lean_attendance: float                      # per unit of this season's games played share
    mean_prior_share: float
    mean_attendance: float
    minutes_prior_games: float                  # the projection's weight on minutes, in games
    minutes_half_life: float                    # games after which a game's minutes count half
    situation_slope: float                      # minutes gained per vacated minute, at the bench end
    own_slope_ridge: float                      # how long the pooled slope outweighs his own games
    usage_slope: float                          # share of the offense gained per vacated possession
    development: Mapping[str, float]            # bucket -> per-minute growth during the season
    prior_weights: Mapping[str, float]          # VOLUME_KEYS in minutes, SHOOTING keys in attempts
    version: str = "unversioned"

    @classmethod
    def from_json(cls, doc: Mapping) -> "Coefficients":
        games = doc["games"]
        minutes = doc["minutes"]
        return cls(
            plays=tuple(tuple(float(v) for v in row) for row in games["plays"]),
            played_once_back=tuple(float(v) for v in games["played_once_back"]),
            lean_prior_share=float(games["lean_prior_share"]),
            lean_attendance=float(games["lean_attendance"]),
            mean_prior_share=float(games["mean_prior_share"]),
            mean_attendance=float(games["mean_attendance"]),
            minutes_prior_games=float(minutes["prior_games"]),
            minutes_half_life=float(minutes["half_life"]),
            situation_slope=float(minutes["situation_slope"]),
            own_slope_ridge=float(minutes["own_slope_ridge"]),
            usage_slope=float(doc["usage_slope"]),
            development={k: float(v) for k, v in (doc.get("development") or {}).items()},
            prior_weights={k: float(v) for k, v in doc["prior_weights"].items()},
            version=str(doc.get("version", "unversioned")),
        )

    def to_json(self) -> dict:
        return {
            "version": self.version,
            "games": {
                "streak_buckets": list(STREAK_BUCKETS),
                "plays": [[round(v, 4) for v in row] for row in self.plays],
                "played_once_back": [round(v, 4) for v in self.played_once_back],
                "lean_prior_share": round(self.lean_prior_share, 4),
                "lean_attendance": round(self.lean_attendance, 4),
                "mean_prior_share": round(self.mean_prior_share, 4),
                "mean_attendance": round(self.mean_attendance, 4),
            },
            "minutes": {
                "prior_games": self.minutes_prior_games,
                "half_life": self.minutes_half_life,
                "situation_slope": self.situation_slope,
                "own_slope_ridge": self.own_slope_ridge,
            },
            "usage_slope": self.usage_slope,
            "development": dict(self.development),
            "prior_weights": dict(self.prior_weights),
        }


# ---- inputs ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BoxLine:
    """One player's regular-season game (nba.player_game_stats)."""

    player_id: int
    team: str
    game_id: str
    game_date: date
    minutes: float
    stats: Mapping[str, float]                  # LINE_KEYS


@dataclass(frozen=True)
class Prior:
    """What was expected of him before the season: the preseason projection, per game."""

    minutes: float
    games: float                                # of a full season
    per_game: Mapping[str, float]               # LINE_KEYS

    @property
    def share(self) -> float:
        return min(max(self.games, 0.0) / SEASON_GAMES, 1.0)

    @property
    def usage(self) -> float:
        return usage_of(self.per_game)


@dataclass(frozen=True)
class Availability:
    """What is known about an absence beyond the box scores."""

    out_next: bool = False                      # ruled out of his team's next game
    return_date: Optional[date] = None          # expected back on this day
    out_for_season: bool = False


@dataclass(frozen=True)
class Member:
    """A team's regular on the morning's roster."""

    player_id: int
    established: float                          # minutes a game
    usage: float                                # possessions a game
    streak: int                                 # team games missed in a row (0: played the last one)
    pre: bool = False                           # has not played this season


@dataclass
class PlayerState:
    """One player as the model sees him this morning."""

    player_id: int
    team: Optional[str]
    minutes: list[float] = field(default_factory=list)      # each game this season, oldest first
    vacated: list[float] = field(default_factory=list)      # regulars' minutes missing from his team that game
    vacated_usage: list[float] = field(default_factory=list)
    current: list[bool] = field(default_factory=list)       # whether the game was for his current team
    totals: dict[str, float] = field(default_factory=dict)  # this season, LINE_KEYS and "min"
    streak: int = 0
    pre: bool = False
    team_played: int = 0
    prior: Optional[Prior] = None
    age: Optional[float] = None

    @property
    def n(self) -> int:
        return len(self.minutes)

    @property
    def established(self) -> float:
        return established(sum(self.minutes), self.n, self.prior.minutes if self.prior else None)


@dataclass
class Morning:
    """A season as known on the morning of `as_of`: games before it are played."""

    as_of: date
    players: dict[int, PlayerState]
    rosters: dict[str, list[Member]]            # each team's regulars
    ahead: dict[str, list[date]]                # each team's remaining game dates, `as_of` included


def established(total: float, games: int, prior: Optional[float]) -> float:
    """A per-game level from this season's games, with the projection worth five more."""
    if prior is not None:
        return (total + ESTABLISHED_GAMES * prior) / (games + ESTABLISHED_GAMES)
    return total / games if games else 0.0


def build_morning(
    lines: Iterable[BoxLine],
    priors: Mapping[int, Prior],
    ahead: Mapping[str, Sequence[date]],
    as_of: date,
    *,
    teams: Optional[Mapping[int, str]] = None,
    ages: Optional[Mapping[int, float]] = None,
) -> Morning:
    """The season so far, with every game filed under who was missing from it.

    A player is on a team's roster on a night when his nearest games around it
    are for that team: the box scores do not carry rosters, and a player who
    did not play has no line. `teams` says where a player is now when the box
    scores cannot: a projected player who has not played yet this season, or
    one traded since his last game. Without it a player belongs to the team he
    last played for.
    """
    teams = teams or {}
    ages = ages or {}
    played = sorted((l for l in lines if l.game_date < as_of), key=lambda l: (l.game_date, l.game_id, l.player_id))

    # each team's games in order, and each player's games in order
    team_games: dict[str, list[tuple[date, str]]] = {}
    for l in played:
        team_games.setdefault(l.team, [])
    seen: set[tuple[str, str]] = set()
    for l in played:
        if (l.team, l.game_id) not in seen:
            seen.add((l.team, l.game_id))
            team_games[l.team].append((l.game_date, l.game_id))
    number = {(team, game_id): i for team, games in team_games.items() for i, (_d, game_id) in enumerate(games)}
    by_player: dict[int, list[BoxLine]] = {}
    for l in played:
        by_player.setdefault(l.player_id, []).append(l)

    # (team, game number) -> minutes and possessions of the regulars who sat it out
    vacated: dict[tuple[str, int], float] = {}
    vacated_usage: dict[tuple[str, int], float] = {}
    # (team) -> the players who sat out its latest game: Member rows, regular or not
    sitting: dict[str, list[Member]] = {}
    latest = {team: len(games) - 1 for team, games in team_games.items()}

    def absent(team: str, game: int, pid: int, minutes: float, usage: float, streak: int, pre: bool) -> None:
        if minutes >= REGULAR_MINUTES:
            vacated[(team, game)] = vacated.get((team, game), 0.0) + minutes
            vacated_usage[(team, game)] = vacated_usage.get((team, game), 0.0) + usage
        if game == latest.get(team):
            sitting.setdefault(team, []).append(Member(pid, minutes, usage, streak, pre))

    final: dict[int, tuple[float, float]] = {}      # player -> established minutes and usage now
    home: dict[int, str] = {}                       # player -> the team he is on this morning
    since: dict[int, int] = {}                      # player -> that team's games since he last played
    for pid, games in by_player.items():
        prior = priors.get(pid)
        pm = prior.minutes if prior else None
        pu = prior.usage if prior else None
        n = 0
        msum = usum = 0.0
        before: list[tuple[float, float]] = []      # established minutes and usage going into each game
        for l in games:
            before.append((established(msum, n, pm), established(usum, n, pu)))
            n += 1
            msum += l.minutes
            usum += usage_of(l.stats)
        final[pid] = (established(msum, n, pm), established(usum, n, pu))

        first = games[0]
        g0 = number[(first.team, first.game_id)]
        if prior is not None:
            # before his first game: out for his team from its opener
            for game in range(g0):
                absent(first.team, game, pid, pm, pu, game + 1, True)
        for i in range(1, len(games)):
            a, b = games[i - 1], games[i]
            ga, gb = number[(a.team, a.game_id)], number[(b.team, b.game_id)]
            est, use = before[i]
            if a.team == b.team:
                for game in range(ga + 1, gb):
                    absent(b.team, game, pid, est, use, game - ga, False)
            else:
                # traded between the two: out for the old team until he plays for the new one
                for game in range(ga + 1, len(team_games[a.team])):
                    if team_games[a.team][game][0] >= b.game_date:
                        break
                    absent(a.team, game, pid, est, use, game - ga, False)

        last = games[-1]
        gl = number[(last.team, last.game_id)]
        est, use = final[pid]
        now = teams.get(pid) or last.team
        home[pid] = now
        if now == last.team:
            for game in range(gl + 1, len(team_games[last.team])):
                absent(last.team, game, pid, est, use, game - gl, False)
            since[pid] = latest[last.team] - gl
        else:
            # moved since his last game: out for the new team's games since then
            missed = 0
            for game, (day, _gid) in enumerate(team_games.get(now, [])):
                if day > last.game_date:
                    missed += 1
                    absent(now, game, pid, est, use, missed, False)
            since[pid] = missed

    # projected players who have not played yet: out for every game of the team they are on
    waiting: dict[int, str] = {}
    for pid, team in teams.items():
        prior = priors.get(pid)
        if pid in by_player or prior is None or team not in team_games:
            continue
        waiting[pid] = team
        for game in range(len(team_games[team])):
            absent(team, game, pid, prior.minutes, prior.usage, game + 1, True)

    rosters: dict[str, list[Member]] = {team: [] for team in team_games}
    for pid, team in home.items():
        if since[pid] == 0 and team in rosters and final[pid][0] >= REGULAR_MINUTES:
            rosters[team].append(Member(pid, final[pid][0], final[pid][1], 0, False))
    for team, members in sitting.items():
        rosters[team].extend(m for m in members if m.established >= REGULAR_MINUTES and (home.get(m.player_id, waiting.get(m.player_id)) == team))

    players: dict[int, PlayerState] = {}
    for pid, games in by_player.items():
        team = home[pid]
        totals = {k: 0.0 for k in LINE_KEYS}
        totals["min"] = 0.0
        state = PlayerState(player_id=pid, team=team, totals=totals, prior=priors.get(pid), age=ages.get(pid),
                            streak=since[pid], team_played=len(team_games.get(team, [])))
        for l in games:
            key = (l.team, number[(l.team, l.game_id)])
            state.minutes.append(float(l.minutes))
            state.vacated.append(vacated.get(key, 0.0))
            state.vacated_usage.append(vacated_usage.get(key, 0.0))
            state.current.append(l.team == team)
            for k in LINE_KEYS:
                totals[k] += float(l.stats.get(k, 0.0))
            totals["min"] += float(l.minutes)
        players[pid] = state
    for pid, team in waiting.items():
        totals = {k: 0.0 for k in LINE_KEYS}
        totals["min"] = 0.0
        played_games = len(team_games[team])
        players[pid] = PlayerState(player_id=pid, team=team, totals=totals, prior=priors[pid], age=ages.get(pid),
                                   streak=played_games, pre=True, team_played=played_games)

    return Morning(
        as_of=as_of, players=players, rosters=rosters,
        ahead={team: sorted(d for d in days if d >= as_of) for team, days in ahead.items()},
    )


# ---- games -------------------------------------------------------------------------------


def play_lean(state: PlayerState, coeffs: Coefficients) -> float:
    """How far his chances sit from his bucket's, as a factor: the share of games
    players like him play once back, leaned on his projected games and on how
    many of this season's he has played, against the bucket's own."""
    b = PRE_DEBUT if state.pre else bucket(state.streak)
    q = coeffs.played_once_back[b]
    if state.prior is None or q <= 0:
        return 1.0
    attended = (
        state.n / max(state.team_played, 1) if (state.n >= 5 and b == 0) else coeffs.mean_attendance
    )
    lean = (coeffs.lean_prior_share * (state.prior.share - coeffs.mean_prior_share)
            + coeffs.lean_attendance * (attended - coeffs.mean_attendance))
    return min(max(q + lean, MIN_PLAY_SHARE), MAX_PLAY_SHARE) / q


def play_chances(
    streak: int,
    pre: bool,
    days: Sequence[date],
    lean: float,
    coeffs: Coefficients,
    known: Optional[Availability] = None,
    as_of: Optional[date] = None,
) -> list[float]:
    """The chance he plays each of his team's remaining games.

    `lean` scales his bucket's curve (1.0: the bucket's own). A player ruled
    out of the next game misses it for certain, and from the one after is a
    player who has missed one more. A known return date replaces the curve for
    a player who is out: nothing before it, the share he plays once back from
    it. One that has passed while he is still out is no longer believed.
    """
    if known is not None and known.out_for_season:
        return [0.0] * len(days)
    b = PRE_DEBUT if pre else bucket(streak)
    out_now = pre or streak > 0
    shift = 0
    if known is not None and known.out_next and days:
        shift = 1
        out_now = True
        if not pre:
            b = bucket(streak + 1)
    back_on = known.return_date if (known is not None and known.return_date is not None and out_now) else None
    if back_on is not None and as_of is not None and back_on < as_of:
        back_on = None
    curve = coeffs.plays[b]
    once_back = min(coeffs.played_once_back[b] * lean, MAX_PLAY_CHANCE)
    chances: list[float] = []
    for k, day in enumerate(days, start=1):
        if shift and k == 1:
            chances.append(0.0)
        elif back_on is not None:
            chances.append(0.0 if day < back_on else once_back)
        else:
            chances.append(min(curve[min(k - shift, HORIZON)] * lean, MAX_PLAY_CHANCE))
    return chances


# ---- role and skill --------------------------------------------------------------------------


def situation_slope(minutes: float, slope: float) -> float:
    """Minutes gained per vacated minute: full for a bench player, nearly none at 34 a night."""
    return slope * min(max((34.0 - minutes) / 16.0, 0.15), 1.0)


@dataclass(frozen=True)
class Role:
    """His minutes this season, as the role part reads them."""

    base: float                 # recent minutes a game
    weight: float               # how many games that is worth
    seen: float                 # vacated minutes in the games it came from, weighted the same
    slope: float                # minutes gained per vacated minute
    prior: float                # the projection's minutes


def role_of(state: PlayerState, coeffs: Coefficients) -> Optional[Role]:
    """None for a player with no game this season."""
    n = state.n
    if n == 0:
        return None
    half = coeffs.minutes_half_life
    weights = [0.5 ** ((n - 1 - j) / half) for j in range(n)] if half else [1.0] * n
    weight = sum(weights)
    base = sum(w * m for w, m in zip(weights, state.minutes)) / weight
    prior = state.prior.minutes if state.prior is not None else base
    here = [(w, v) for w, v, c in zip(weights, state.vacated, state.current) if c]
    if not here or not coeffs.situation_slope:
        return Role(base=base, weight=weight, seen=0.0, slope=0.0, prior=prior)
    seen = sum(w * v for w, v in here) / sum(w for w, _v in here)
    slope = situation_slope(
        (base * weight + ESTABLISHED_GAMES * prior) / (weight + ESTABLISHED_GAMES), coeffs.situation_slope
    )
    if coeffs.own_slope_ridge and len(here) >= 6:
        vs = [v for v, c in zip(state.vacated, state.current) if c]
        ms = [m for m, c in zip(state.minutes, state.current) if c]
        v_mean, m_mean = sum(vs) / len(vs), sum(ms) / len(ms)
        cov = sum((v - v_mean) * (m - m_mean) for v, m in zip(vs, ms))
        var = sum((v - v_mean) ** 2 for v in vs)
        slope = (cov + coeffs.own_slope_ridge * slope) / (var + coeffs.own_slope_ridge)
    return Role(base=base, weight=weight, seen=seen, slope=slope, prior=prior)


def minutes_for(role: Optional[Role], state: PlayerState, missing: Optional[float], coeffs: Coefficients) -> float:
    """His minutes in a game where `missing` minutes of the team's other regulars are
    expected to be out (net of the league-wide drift; None for "as in his games so far")."""
    if role is None:
        return state.prior.minutes if state.prior is not None else 0.0
    moved = role.slope * (missing - role.seen) if (missing is not None and role.slope) else 0.0
    k = coeffs.minutes_prior_games
    return min(max(((role.base + moved) * role.weight + k * role.prior) / (role.weight + k), 0.0), MAX_MINUTES)


def development_bucket(age: Optional[float]) -> Optional[str]:
    """The in-season development bucket for a player of `age`; None past 24."""
    if age is None:
        return None
    for name, limit in DEVELOPING:
        if age <= limit:
            return name
    return None


def development_of(age: Optional[float], coeffs: Coefficients) -> float:
    name = development_bucket(age)
    return 1.0 + float(coeffs.development.get(name, 0.0)) if name else 1.0


def _with_twos(totals: Mapping[str, float]) -> dict[str, float]:
    out = dict(totals)
    out["fg2m"] = out.get("fgm", 0.0) - out.get("fg3m", 0.0)
    out["fg2a"] = out.get("fga", 0.0) - out.get("fg3a", 0.0)
    return out


def prior_totals(prior: Prior) -> dict[str, float]:
    """The projection as season totals: what the prior weights are counted against."""
    games = max(prior.games, 1.0)
    out = {k: float(prior.per_game.get(k, 0.0)) * games for k in LINE_KEYS}
    out["min"] = prior.minutes * games
    return _with_twos(out)


def rates_of(state: PlayerState, coeffs: Coefficients, generic: Optional[Mapping[str, float]] = None) -> dict[str, float]:
    """His per-minute line at his usual share of the offense: this season's games
    against the projection, a weight per stat. `generic` is the pool's projection
    totals, the prior of a player nothing projects."""
    season = _with_twos(state.totals)
    if state.prior is not None:
        prior = prior_totals(state.prior)
    elif generic is not None:
        prior = dict(generic)
    else:
        prior = season
    prior_minutes = max(prior.get("min", 0.0), 1.0)
    grow = development_of(state.age, coeffs)
    weights = coeffs.prior_weights
    rate: dict[str, float] = {}
    for k in VOLUME_KEYS:
        w = float(weights[k])
        rate[k] = (season.get(k, 0.0) + w * prior.get(k, 0.0) / prior_minutes) / (season.get("min", 0.0) + w)
        if k != "tov":
            rate[k] *= grow
    fg2a = max(rate["fga"] - rate["fg3a"], 0.0)
    pct: dict[str, float] = {}
    for name, (made, tried) in SHOOTING.items():
        w = float(weights[name])
        before = (prior.get(made, 0.0) + PCT_PAD * LEAGUE_PCT[name]) / (prior.get(tried, 0.0) + PCT_PAD)
        pct[name] = (season.get(made, 0.0) + w * before) / (season.get(tried, 0.0) + w)
    fg2m, fg3m, ftm = fg2a * pct["fg2"], rate["fg3a"] * pct["fg3"], rate["fta"] * pct["ft"]
    return {
        "pts": 2.0 * fg2m + 3.0 * fg3m + ftm,
        "reb": rate["reb"], "ast": rate["ast"], "stl": rate["stl"], "blk": rate["blk"], "tov": rate["tov"],
        "fgm": fg2m + fg3m, "fga": rate["fga"], "fg3m": fg3m, "fg3a": rate["fg3a"], "ftm": ftm, "fta": rate["fta"],
    }


def usage_seen(state: PlayerState) -> Optional[float]:
    """Possessions of the team's regulars missing from his games for his current team, a game."""
    here = [u for u, c in zip(state.vacated_usage, state.current) if c]
    return sum(here) / len(here) if here else None


def vacated_seen(state: PlayerState) -> Optional[float]:
    here = [v for v, c in zip(state.vacated, state.current) if c]
    return sum(here) / len(here) if here else None


# ---- the projection ----------------------------------------------------------------------------


@dataclass(frozen=True)
class GameAhead:
    """One remaining game of his team's."""

    day: date
    chance: float               # that he plays it
    minutes: float              # if he does
    usage: float                # his share of the offense against his usual one (1.0: as usual)


@dataclass
class LiveProjection:
    player_id: int
    team: Optional[str]
    games: float                                    # games he is expected to play from here
    minutes: float                                  # a game, when he plays
    per_game: dict[str, float]                      # LINE_KEYS, when he plays
    rates: dict[str, float]                         # per minute, at his usual share of the offense
    games_ahead: list[GameAhead] = field(default_factory=list)
    parts: dict = field(default_factory=dict)       # what each part read, for the reasons shown later

    def line(self) -> dict[str, float]:
        """The row nba.player_projections stores: LINE_KEYS plus min, per game."""
        out = {k: round(self.per_game.get(k, 0.0), 2) for k in LINE_KEYS}
        out["min"] = round(self.minutes, 2)
        return out

    def game_line(self, game: GameAhead) -> dict[str, float]:
        """His line in one remaining game, if he plays it."""
        return {
            k: self.rates[k] * game.minutes * (game.usage if k in USAGE_LINE_KEYS else 1.0)
            for k in LINE_KEYS
        }

    def window(self, start: date, end: date) -> tuple[float, dict[str, float]]:
        """Expected games and stat totals from `start` to `end`, both included."""
        games = 0.0
        totals = {k: 0.0 for k in LINE_KEYS}
        for game in self.games_ahead:
            if start <= game.day <= end:
                games += game.chance
                for k, v in self.game_line(game).items():
                    totals[k] += game.chance * v
        return games, totals

    def timeline(self) -> list[list]:
        """`games_ahead` as runs of alike games: [first day, last day, games, chance,
        minutes, usage]. A healthy player on a healthy team is one run."""
        return compress(self.games_ahead)


TIMELINE_CHANCE_STEP = 0.04
TIMELINE_MINUTES_STEP = 0.6
TIMELINE_USAGE_STEP = 0.01


def compress(games: Sequence[GameAhead]) -> list[list]:
    runs: list[list[GameAhead]] = []
    for game in games:
        head = runs[-1][0] if runs else None
        if (
            head is not None
            and abs(game.chance - head.chance) <= TIMELINE_CHANCE_STEP
            and abs(game.minutes - head.minutes) <= TIMELINE_MINUTES_STEP
            and abs(game.usage - head.usage) <= TIMELINE_USAGE_STEP
        ):
            runs[-1].append(game)
        else:
            runs.append([game])
    out: list[list] = []
    for run in runs:
        n = len(run)
        out.append([
            run[0].day.isoformat(), run[-1].day.isoformat(), n,
            round(sum(g.chance for g in run) / n, 3),
            round(sum(g.minutes for g in run) / n, 1),
            round(sum(g.usage for g in run) / n, 3),
        ])
    return out


def expand(timeline: Sequence[Sequence], days: Sequence[date]) -> list[GameAhead]:
    """A stored timeline back onto his team's game days: each day takes the run
    it falls in (the nearest earlier one when the schedule has moved)."""
    runs = [(date.fromisoformat(r[0]), date.fromisoformat(r[1]), float(r[3]), float(r[4]), float(r[5])) for r in timeline]
    out: list[GameAhead] = []
    for day in days:
        chosen = None
        for run in runs:
            if run[0] <= day:
                chosen = run
            if run[0] <= day <= run[1]:
                break
        if chosen is None:
            if not runs:
                continue
            chosen = runs[0]
        out.append(GameAhead(day=day, chance=chosen[2], minutes=chosen[3], usage=chosen[4]))
    return out


@dataclass
class Outlook:
    """What a player's remaining games look like before any minutes are priced:
    the chance he plays each, and the regulars' minutes and possessions expected
    to be missing around him, net of the league-wide drift."""

    chances: list[float]
    missing: list[float]
    missing_usage: list[float]

    @property
    def games(self) -> float:
        return sum(self.chances)

    def average(self, values: Sequence[float]) -> Optional[float]:
        """`values` averaged over the games he is expected to play."""
        total = sum(self.chances)
        if total > 0:
            return sum(c * v for c, v in zip(self.chances, values)) / total
        return sum(values) / len(values) if values else None


def outlooks(
    morning: Morning,
    coeffs: Coefficients,
    known: Optional[Mapping[int, Availability]] = None,
) -> dict[int, Outlook]:
    """Every player's `Outlook`.

    The drift: the minutes expected to be missing from a team's remaining games
    and the minutes that were missing from its games so far are counted in two
    ways, so across the league they differ a little even when no one's
    situation has changed. That league-wide difference is taken out.
    """
    known = known or {}
    # each team's regulars: the chance each misses each remaining game
    team_missing: dict[str, list[float]] = {}
    team_usage: dict[str, list[float]] = {}
    own: dict[int, tuple[list[float], float, float]] = {}
    for team, members in morning.rosters.items():
        days = morning.ahead.get(team, [])
        total = [0.0] * len(days)
        usage = [0.0] * len(days)
        for m in members:
            chances = play_chances(m.streak, m.pre, days, 1.0, coeffs, known.get(m.player_id), morning.as_of)
            misses = [1.0 - c for c in chances]
            own[m.player_id] = (misses, m.established, m.usage)
            for i, miss in enumerate(misses):
                total[i] += miss * m.established
                usage[i] += miss * m.usage
        team_missing[team], team_usage[team] = total, usage

    raw: dict[int, tuple[list[float], list[float], list[float]]] = {}
    for pid, state in morning.players.items():
        days = morning.ahead.get(state.team, []) if state.team else []
        chances = play_chances(state.streak, state.pre, days, play_lean(state, coeffs), coeffs,
                               known.get(pid), morning.as_of)
        missing = list(team_missing.get(state.team, [0.0] * len(days)))
        usage = list(team_usage.get(state.team, [0.0] * len(days)))
        mine = own.get(pid)
        if mine is not None and len(mine[0]) == len(days):
            missing = [v - miss * mine[1] for v, miss in zip(missing, mine[0])]
            usage = [u - miss * mine[2] for u, miss in zip(usage, mine[0])]
        raw[pid] = (chances, missing, usage)

    # the league-wide drift, over players with a projection and a game for their team
    gaps: list[float] = []
    usage_gaps: list[float] = []
    for pid, (_chances, missing, usage) in raw.items():
        state = morning.players[pid]
        seen, seen_usage = vacated_seen(state), usage_seen(state)
        if state.prior is None or seen is None or not missing:
            continue
        reach = min(len(missing), HORIZON)
        gaps.append(sum(missing[:reach]) / reach - seen)
        usage_gaps.append(sum(usage[:reach]) / reach - seen_usage)
    drift = sum(gaps) / len(gaps) if gaps else 0.0
    drift_usage = sum(usage_gaps) / len(usage_gaps) if usage_gaps else 0.0

    return {
        pid: Outlook(
            chances=chances,
            missing=[v - drift for v in missing],
            missing_usage=[u - drift_usage for u in usage],
        )
        for pid, (chances, missing, usage) in raw.items()
    }


def generic_prior(morning: Morning) -> Optional[dict[str, float]]:
    """The pool's projections summed: the per-minute prior of a player nothing projects."""
    total: dict[str, float] = {}
    for state in morning.players.values():
        if state.prior is None:
            continue
        for k, v in prior_totals(state.prior).items():
            total[k] = total.get(k, 0.0) + v
    return total or None


def usage_factor(state: PlayerState, missing_usage: Optional[float], coeffs: Coefficients) -> float:
    """His share of the offense in a game, against his usual one."""
    seen = usage_seen(state)
    if seen is None or missing_usage is None or not coeffs.usage_slope:
        return 1.0
    return 1.0 + coeffs.usage_slope * (missing_usage - seen)


def project_player(
    state: PlayerState,
    outlook: Outlook,
    days: Sequence[date],
    coeffs: Coefficients,
    generic: Optional[Mapping[str, float]] = None,
) -> LiveProjection:
    role = role_of(state, coeffs)
    rates = rates_of(state, coeffs, generic)
    ahead = [
        GameAhead(
            day=day, chance=chance,
            minutes=minutes_for(role, state, missing, coeffs),
            usage=usage_factor(state, missing_usage, coeffs),
        )
        for day, chance, missing, missing_usage in zip(days, outlook.chances, outlook.missing, outlook.missing_usage)
    ]
    out = LiveProjection(player_id=state.player_id, team=state.team, games=outlook.games, minutes=0.0,
                         per_game={}, rates=rates, games_ahead=ahead)
    weights = outlook.chances if outlook.games > 0 else [1.0] * len(ahead)
    total = sum(weights)
    if ahead and total > 0:
        out.minutes = sum(w * g.minutes for w, g in zip(weights, ahead)) / total
        out.per_game = {
            k: sum(w * v[k] for w, v in zip(weights, (out.game_line(g) for g in ahead))) / total for k in LINE_KEYS
        }
    else:
        # no game left on the calendar: his line as it stands
        out.minutes = minutes_for(role, state, None, coeffs)
        out.per_game = {k: rates[k] * out.minutes for k in LINE_KEYS}
    seen = vacated_seen(state)
    out.parts = {
        "games_played": state.n,
        "season_minutes": round(state.totals.get("min", 0.0) / state.n, 1) if state.n else None,
        "recent_minutes": round(role.base, 1) if role is not None else None,
        "prior_minutes": round(state.prior.minutes, 1) if state.prior is not None else None,
        "prior_games": round(state.prior.games, 1) if state.prior is not None else None,
        "missing_so_far": round(seen, 1) if seen is not None else None,
        "missing_ahead": round(outlook.average(outlook.missing) or 0.0, 1) if outlook.missing else None,
        "slope": round(role.slope, 3) if role is not None else None,
        "streak": state.streak,
        "not_yet_played": state.pre,
        "team_games_left": len(days),
    }
    return out


def average_line(
    state: PlayerState,
    outlook: Outlook,
    coeffs: Coefficients,
    generic: Optional[Mapping[str, float]] = None,
) -> tuple[float, dict[str, float]]:
    """His minutes and line straight from the outlook's averages: `project_player`'s
    answer whenever no single game's minutes hit a bound. The fit reads this one,
    thousands of times; what is published and what is scored is `project_player`."""
    minutes = minutes_for(role_of(state, coeffs), state, outlook.average(outlook.missing), coeffs)
    usage = usage_factor(state, outlook.average(outlook.missing_usage), coeffs)
    rates = rates_of(state, coeffs, generic)
    return minutes, {k: rates[k] * minutes * (usage if k in USAGE_LINE_KEYS else 1.0) for k in LINE_KEYS}


def project(
    morning: Morning,
    coeffs: Coefficients,
    known: Optional[Mapping[int, Availability]] = None,
) -> dict[int, LiveProjection]:
    """Every player's live projection for the morning."""
    looks = outlooks(morning, coeffs, known)
    generic = generic_prior(morning)
    out: dict[int, LiveProjection] = {}
    for pid, state in morning.players.items():
        days = morning.ahead.get(state.team, []) if state.team else []
        out[pid] = project_player(state, looks[pid], days, coeffs, generic)
        info = (known or {}).get(pid)
        if info is not None:
            out[pid].parts["known"] = {
                "out_next": info.out_next, "out_for_season": info.out_for_season,
                "return_date": info.return_date.isoformat() if info.return_date else None,
            }
    return out
