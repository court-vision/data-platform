# Court Vision — Data Platform

Standalone ETL service for the Court Vision fantasy basketball platform. Handles all data pipelines: fetching from the NBA API and ESPN, transforming stats, and writing to the shared PostgreSQL database. The backend API reads from that database but never calls pipelines directly — all pipeline triggering goes through this service.

Runs on port **8001** (private, Railway internal network) with a separate public dashboard on **8080**.

## System Context

```
cron-runner  ──POST──►  data-platform (port 8001)  ──writes──►  PostgreSQL
                                                                      │
                                                              backend (port 8000)
                                                                      │
                                                               frontend (Next.js)
```

- **cron-runner** (Go) fires scheduled HTTP POSTs to trigger pipeline groups
- **data-platform** fetches from NBA API / ESPN / BALLDONTLIE, transforms data, upserts to PostgreSQL
- **backend** reads from PostgreSQL to serve user-facing API routes — no ETL code lives there

## Tech Stack

| Layer | Technology |
|---|---|
| Web framework | FastAPI 0.128, Uvicorn |
| ORM / DB | Peewee 3.18, psycopg2-binary, PostgreSQL |
| Data validation | Pydantic v2, pydantic-settings |
| NBA data | nba_api 1.9, curl-cffi |
| Data processing | pandas, numpy |
| Resilience | tenacity (retry), circuitbreaker |
| Logging | structlog (JSON + console modes) |
| Email alerts | Resend |
| Auth | Bearer token (`PIPELINE_API_TOKEN`) |
| Container | Docker (python:3.12-slim-bookworm) |

## Directory Structure

```
data-platform/
├── main.py                  # Private app (port 8001) — all routes
├── main_public.py           # Public app (port 8080) — dashboard + triggers only
├── entrypoint.sh            # Starts both servers in the same container
├── Dockerfile
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
│
├── api/v1/
│   ├── pipelines.py         # Pipeline trigger endpoints (/v1/internal/pipelines/*)
│   ├── live.py              # Live schedule endpoint for cron-runner (/v1/live/*)
│   ├── dashboard.py         # Pipeline monitoring dashboard (/v1/dashboard/*)
│   └── quality.py           # Data quality check endpoints (/v1/internal/quality/*)
│
├── pipelines/
│   ├── __init__.py          # PIPELINE_REGISTRY + run_pipeline / run_all helpers
│   ├── base.py              # BasePipeline — template method, async execution
│   ├── config.py            # PipelineConfig dataclass, PipelineCategory enum
│   ├── context.py           # PipelineContext — run tracking, logging, timing
│   ├── player_game_stats.py
│   ├── player_season_stats.py
│   ├── player_advanced_stats.py
│   ├── player_ownership.py
│   ├── player_rolling_stats.py
│   ├── player_profiles.py
│   ├── team_stats.py
│   ├── game_schedule.py
│   ├── game_start_times.py
│   ├── daily_matchup_scores.py
│   ├── live_game_stats.py
│   ├── espn_injury_status.py
│   ├── breakout_detection.py
│   ├── lineup_alerts.py
│   ├── extractors/
│   │   ├── base.py          # BaseExtractor
│   │   ├── nba_api.py       # NBAApiExtractor (PlayerGameLogs, BoxScore, etc.)
│   │   ├── espn.py          # ESPNExtractor (ownership, matchup scores)
│   │   ├── injuries.py      # BALLDONTLIE injury extractor
│   │   └── yahoo.py         # YahooExtractor
│   └── transformers/
│       ├── fantasy_points.py  # Fantasy point scoring logic
│       └── names.py           # Player name normalization
│
├── core/
│   ├── settings.py          # Pydantic Settings — all env vars with defaults
│   ├── logging.py           # structlog setup, get_logger(), correlation IDs
│   ├── resilience.py        # @with_retry, circuit breakers, ResilientHTTPClient
│   ├── job_manager.py       # In-memory background job tracking
│   ├── pipeline_auth.py     # Bearer token verification
│   ├── middleware.py        # CORS, request logging
│   ├── db_middleware.py     # Per-request DB connection management
│   └── correlation_middleware.py  # X-Correlation-ID header injection
│
├── db/
│   ├── base.py              # PooledPostgresqlDatabase, init_db(), close_db()
│   └── models/
│       ├── pipeline_run.py  # Audit log for every pipeline execution
│       ├── data_quality_run.py / data_quality_check.py
│       ├── nba/             # Player, NBATeam, PlayerGameStats, PlayerSeasonStats,
│       │                    # PlayerOwnership, PlayerRollingStats, PlayerAdvancedStats,
│       │                    # PlayerProfile, PlayerInjury, Game, LivePlayerStats,
│       │                    # BreakoutCandidate, TeamStats
│       ├── stats/           # DailyMatchupScore (legacy stats_s2 schema)
│       ├── notifications.py # NotificationPreference, NotificationLog
│       └── (users, teams, lineups, etc.)
│
├── schemas/                 # Pydantic response models (pipeline, dashboard, quality)
├── services/
│   ├── schedule_service.py  # NBA matchup schedule helpers (reads static JSON)
│   ├── data_quality_service.py
│   ├── lineup_check_service.py
│   └── notification_service.py
├── utils/
│   ├── patches.py           # Monkey-patches nba_api (applied before any imports)
│   ├── constants.py
│   ├── espn_helpers.py
│   ├── etl_helpers.py
│   └── yahoo_helpers.py
├── static/                  # NBA schedule JSON files
├── templates/               # Jinja2 templates (dashboard HTML)
├── tests/
└── scripts/                 # One-off utility scripts
```

## Setup & Installation

```bash
cd data-platform

# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install runtime dependencies
pip install -r requirements.txt

# Install the local package (db/, tasks/, utils/, core/ as importable packages)
pip install -e .

# Install dev/test dependencies
pip install -r requirements-dev.txt
```

## Environment Variables

Copy `secrets.env` to `.env` (or export directly). All settings are in `core/settings.py`.

| Variable | Required | Default | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | — | PostgreSQL connection URL (e.g. `postgresql://user:pass@host/db`) |
| `PIPELINE_API_TOKEN` | Yes | — | Bearer token required by all `/v1/internal/*` endpoints |
| `ESPN_YEAR` | No | `2026` | ESPN Fantasy season year |
| `ESPN_LEAGUE_ID` | No | `993431466` | ESPN Fantasy league ID |
| `NBA_SEASON` | No | `"2025-26"` | nba_api season string |
| `BALLDONTLIE_API_KEY` | No | — | BALLDONTLIE API key for injury data |
| `RESEND_API_KEY` | No | — | Resend API key for lineup alert emails |
| `NOTIFICATION_FROM_EMAIL` | No | `alerts@courtvision.dev` | Sender address for alerts |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |
| `LOG_FORMAT` | No | `json` | `json` (production) or `console` (development) |
| `SERVICE_NAME` | No | `court-vision-data-platform` | Appears in all log entries |
| `RETRY_MAX_ATTEMPTS` | No | `3` | Retry attempts for external API calls |
| `RETRY_BASE_DELAY` | No | `2.0` | Exponential backoff base (seconds) |
| `CIRCUIT_BREAKER_THRESHOLD` | No | `5` | Failures before circuit opens |
| `DEVELOPMENT_MODE` | No | `false` | Enables dev-mode shortcuts |
| `PRIVATE_PORT` | No | `8001` | Port for the private (internal) server |
| `PORT` | No | `8080` | Port for the public (dashboard) server |

## Running Locally

### Single server (development)

```bash
source .venv/bin/activate
uvicorn main:app --reload --host 0.0.0.0 --port 8001
```

API docs available at `http://localhost:8001/docs`.

### Both servers (matches production)

```bash
source .venv/bin/activate
./entrypoint.sh
```

This starts two Uvicorn processes:
- **Private** (`::8001`) — full app with all routes (pipeline triggers, live schedule, dashboard, quality)
- **Public** (`0.0.0.0:8080`) — dashboard and pipeline triggers only, no live route, no API docs

### Running Tests

```bash
source .venv/bin/activate
pytest                        # Run all tests
pytest tests/pipelines/       # Run pipeline tests only
pytest --cov=pipelines        # With coverage
./run_tests.sh                # If the script exists
```

## API Endpoints

All `/v1/internal/*` endpoints require the `Authorization: Bearer <PIPELINE_API_TOKEN>` header.

### Pipeline Triggers

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/internal/pipelines/` | List all registered pipelines |
| `POST` | `/v1/internal/pipelines/all` | Fire-and-forget: run all non-SCHEDULED pipelines, return job ID |
| `POST` | `/v1/internal/pipelines/post-game` | Run all `POST_GAME` pipelines (after games complete) |
| `POST` | `/v1/internal/pipelines/pre-game` | Run all `PRE_GAME` pipelines (before tip-off) |
| `POST` | `/v1/internal/pipelines/live-stats` | Run `LIVE` pipeline once; returns `all_games_complete` flag |
| `POST` | `/v1/internal/pipelines/daily-player-stats` | Individual: player game stats |
| `POST` | `/v1/internal/pipelines/cumulative-player-stats` | Individual: season totals |
| `POST` | `/v1/internal/pipelines/player-advanced-stats` | Individual: advanced metrics |
| `POST` | `/v1/internal/pipelines/player-ownership` | Individual: ESPN ownership % |
| `POST` | `/v1/internal/pipelines/player-rolling-stats` | Individual: rolling averages |
| `POST` | `/v1/internal/pipelines/team-stats` | Individual: team-level stats |
| `POST` | `/v1/internal/pipelines/game-schedule` | Individual: NBA game schedule |
| `POST` | `/v1/internal/pipelines/game-start-times` | Individual: game tip-off times |
| `POST` | `/v1/internal/pipelines/daily-matchup-scores` | Individual: ESPN matchup scores |
| `POST` | `/v1/internal/pipelines/espn-injury-status` | Individual: ESPN injury status |
| `POST` | `/v1/internal/pipelines/breakout-detection` | Individual: breakout candidate detection |
| `POST` | `/v1/internal/pipelines/lineup-alerts` | Individual: email lineup alerts |
| `POST` | `/v1/internal/pipelines/player-profiles` | Individual: player profile data |

All individual trigger endpoints accept an optional `?date=YYYY-MM-DD` query param for backfills.

### Background Job Status

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/internal/pipelines/jobs` | List recent background jobs (from `/all`) |
| `GET` | `/v1/internal/pipelines/jobs/{job_id}` | Get status and per-pipeline results for a job |

### Live Schedule (no auth)

| Method | Path | Description |
|---|---|---|
| `GET` | `/v1/live/schedule/today` | First tip-off time for today; used by cron-runner live loop to compute sleep time |

### Dashboard

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/v1/dashboard` | None | Pipeline monitoring UI (HTML) |
| `GET` | `/v1/dashboard/status` | Token | Pipeline health + recent jobs JSON |

### Data Quality

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/internal/quality/run` | Run SQL data quality checks |
| `GET` | `/v1/internal/quality/runs` | List recent quality runs |
| `GET` | `/v1/internal/quality/runs/{run_id}` | Get details for a specific quality run |

### Health

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Service info |
| `GET` | `/ping` | Liveness check |

## Pipeline Architecture

### Execution Model

Every pipeline extends `BasePipeline` and implements a single `execute(ctx)` method. The base class handles everything else:

1. **`pipeline.run()`** — async entry point; delegates to `asyncio.to_thread(_run_sync)` so blocking I/O never stalls the event loop
2. **`_run_sync()`** — opens a thread-local DB connection, instantiates a `PipelineContext`, calls the lifecycle hooks, closes the connection
3. **`execute(ctx)`** — the pipeline's logic: fetch from external APIs, transform, upsert to PostgreSQL

```python
class MyPipeline(BasePipeline):
    config = PipelineConfig(
        name="my_pipeline",
        display_name="My Pipeline",
        description="Fetches X and writes to Y",
        target_table="nba.my_table",
        category=PipelineCategory.POST_GAME,
    )

    def execute(self, ctx: PipelineContext) -> None:
        data = fetch_from_somewhere()
        ctx.increment_records(len(data))
        upsert_to_db(data)
```

### Pipeline Categories

| Category | Trigger | Cron-runner job |
|---|---|---|
| `POST_GAME` | After all games are final (~2 AM ET) | `JOB=trigger` (post-game) |
| `PRE_GAME` | ~150 min before first tip-off | `JOB=trigger` (pre-game) |
| `LIVE` | Every 60s during active game windows | `JOB=loop` (live) |
| `SCHEDULED` | Fixed Railway cron, each has its own service | varies |

### Self-Gating

Category endpoints self-gate — the service decides whether work is needed:

- **pre-game**: checks that current time is within the configured window before tip-off
- **post-game**: checks that all games are final; `espn_gated=True` pipelines additionally wait for ESPN's `latestScoringPeriod` to advance (with a 2:30 AM ET fallback)
- **live-stats**: checks NBA game schedule; returns `all_games_complete: true` when the cron loop should exit

### PipelineContext

`PipelineContext` is passed to `execute()` and provides:

- `ctx.log` — bound structlog logger with `pipeline` and `run_id` fields
- `ctx.increment_records(n)` — counter for records upserted
- `ctx.date_override` — backfill date (None = use today)
- Auto-creates a `pipeline_run` audit record on start; marks success/failure on completion

### Audit Trail

Every pipeline execution writes a record to `nba.pipeline_run` with:
- Pipeline name, run ID (UUID), status (`running` / `success` / `failed`)
- Start time, end time, duration, records processed
- Error message + traceback on failure

Stale `running` records (from crashed processes) are reset to `failed` on startup.

### Resilience

- **Retries**: `@with_retry(max_attempts=3)` decorator using tenacity with exponential backoff on `RetryableError` subclasses (`RateLimitError`, `NetworkError`, `ServerError`)
- **Circuit breakers**: `nba_api_circuit` and `espn_api_circuit` open after 5 consecutive failures, recover after 60s
- **HTTP client**: `ResilientHTTPClient` combines retry + circuit breaker with classified error types

### Extractors

Extractors in `pipelines/extractors/` wrap external data sources:

- `NBAApiExtractor` — wraps `nba_api` endpoints (PlayerGameLogs, BoxScoreAdvanced, LeagueLeaders, live BoxScore)
- `ESPNExtractor` — ESPN Fantasy API (ownership percentages, matchup scores, roster data)
- `InjuriesExtractor` — BALLDONTLIE API for injury reports
- `YahooExtractor` — Yahoo Fantasy API

### Background Jobs (`/all` endpoint)

The `/v1/internal/pipelines/all` endpoint returns immediately with a `job_id`. Pipelines run in the background sequentially. Use `GET /v1/internal/pipelines/jobs/{job_id}` to poll status. The in-memory `JobManager` keeps the last 100 jobs; jobs are lost on restart.

## Database Schema Overview

All models use Peewee ORM. `init_db()` in `db/base.py` runs `create_tables(safe=True)` on startup (idempotent).

Key tables written by this service:

| Table | Pipeline |
|---|---|
| `nba.player_game_stats` | PlayerGameStatsPipeline |
| `nba.player_season_stats` | PlayerSeasonStatsPipeline |
| `nba.player_advanced_stats` | PlayerAdvancedStatsPipeline |
| `nba.player_ownership` | PlayerOwnershipPipeline |
| `nba.player_rolling_stats` | PlayerRollingStatsPipeline |
| `nba.player_profiles` | PlayerProfilesPipeline |
| `nba.team_stats` | TeamStatsPipeline |
| `nba.games` | GameSchedulePipeline, GameStartTimesPipeline |
| `nba.live_player_stats` | LiveGameStatsPipeline |
| `nba.player_injuries` | ESPNInjuryStatusPipeline |
| `nba.breakout_candidates` | BreakoutDetectionPipeline |
| `nba.pipeline_run` | All pipelines (audit) |
| `stats_s2.daily_matchup_scores` | DailyMatchupScoresPipeline |

## Adding a New Pipeline

1. Create `pipelines/my_pipeline.py` extending `BasePipeline` with a `config` class attribute
2. Implement `execute(self, ctx: PipelineContext) -> None`
3. Register it in `pipelines/__init__.py` — add an import and an entry in `PIPELINE_REGISTRY`
4. Add a trigger endpoint in `api/v1/pipelines.py` (or use the category-based endpoint if it fits)
5. Add the DB model to `db/models/nba/` and register it in `db/base.py:init_db()`

## Production Deployment

Deployed on Railway. The container runs `entrypoint.sh`, which starts:

- **Private server** (`::8001`, IPv6) — reachable only on Railway's internal network by cron-runner and backend
- **Public server** (`0.0.0.0:$PORT`) — routed from `data.courtvision.dev`; serves the dashboard UI only

Environment variables are set as Railway service variables. `DATABASE_URL` uses Railway's private PostgreSQL URL.
