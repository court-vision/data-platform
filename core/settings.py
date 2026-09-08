"""
Centralized Settings Configuration

Uses Pydantic Settings to load configuration from environment variables
with validation and type coercion.
"""

from typing import Optional
from urllib.parse import urlsplit
from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from core.season import espn_year_for, season_key, validate_season


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    database_url: str

    # Season. Both default to the current season derived from today's date
    # (flips on Aug 1); set NBA_SEASON / ESPN_YEAR to pin them.
    nba_season: str = ""
    espn_year: int = 0

    # ESPN Configuration
    espn_league_id: int = 993431466

    # BALLDONTLIE API (for injury data)
    # Get a free key at https://app.balldontlie.io
    balldontlie_api_key: Optional[SecretStr] = None

    # Residential proxy for stats.nba.com (cloud IPs are often blocked)
    # Format: http://username:password@host:port
    nba_api_proxy_url: Optional[str] = None

    # Resilience
    retry_max_attempts: int = 3
    retry_base_delay: float = 2.0
    retry_max_delay: float = 30.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    http_timeout: int = 30

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"  # "json" or "console"
    service_name: str = "court-vision-data-platform"

    # Pipeline Auth
    pipeline_api_token: SecretStr

    # GitHub repository_dispatch for nightly production deploys
    # Token requires repo + workflow scopes. Repos are the GitHub repos for each service.
    github_deploy_token: Optional[SecretStr] = None
    backend_github_repo: Optional[str] = None        # e.g. "username/backend"
    data_platform_github_repo: Optional[str] = None  # e.g. "username/data-platform"

    # Resend (email notifications)
    resend_api_key: Optional[SecretStr] = None
    notification_from_email: str = "alerts@courtvision.dev"
    lineup_alert_window_minutes: int = 150  # broad outer gate; must be >= max user-configurable value (150)

    # Backend internal API. The lineup-alerts pipeline asks the backend for each
    # opted-in team's fill plan (POST /v1/internal/jobs/lineup/evaluate; the
    # backend owns the ESPN roster read, eligibility, locks and the planner, and
    # applies the plan for auto-lineup users). Authenticated with the same
    # PIPELINE_API_TOKEN both services share. Unset -> the pipeline logs
    # `backend_not_configured` and does nothing for any team.
    # Railway private networking only (validator below):
    #   production: http://api.railway.internal:8080
    #   staging:    http://api-staging.railway.internal:8080
    backend_internal_url: Optional[str] = None
    # Read timeout per evaluate call: > backend roster read + 30 s writer call + re-read.
    backend_timeout_seconds: float = 45.0

    # Pre-game pipeline scheduling
    pre_game_window_minutes: int = 150  # how many minutes before first tip-off pre-game pipelines become eligible

    # Post-game pipeline scheduling
    estimated_game_duration_minutes: int = 150  # time added to latest game start to estimate end (~2.5hr)
    post_game_pipeline_window_minutes: int = 210  # window after estimated end to attempt trigger
    # Retry budget per night for ESPN-gated pipelines. The gate used to return
    # "run" on every ESPN error, so an outage meant a failing run every 15
    # minutes for the whole window; this bounds it. See pipelines/gates.py.
    # 6 rather than 3 since the write-side pairing guard landed: a run that
    # skips because ESPN's period advanced ahead of its totals still counts
    # against this budget, so the budget is also the tolerance for that lag
    # (6 x 15 min polls = ~90 minutes of period-before-totals slack).
    espn_gate_max_attempts: int = 6

    # Development mode
    development_mode: bool = False

    # Deployment metadata (Railway injects these at runtime; unset locally).
    # Deploys go through `railway up` (a tarball upload), which does NOT set
    # RAILWAY_GIT_COMMIT_SHA; the deploy workflow sets APP_VERSION on the service
    # right before `railway up` instead. Version resolution: APP_VERSION, then
    # RAILWAY_GIT_COMMIT_SHA[:7], then "dev".
    app_version: Optional[str] = None
    railway_git_commit_sha: Optional[str] = None
    railway_environment_name: Optional[str] = None
    railway_service_name: Optional[str] = None

    # Sentry. No DSN -> SDK not initialised (dev, tests).
    sentry_dsn: Optional[SecretStr] = None
    sentry_environment: Optional[str] = None  # defaults to the Railway environment name, else "development"
    sentry_traces_sample_rate: float = 0.0

    # Ops alerts: one Discord (or Slack) incoming webhook for #cv-alerts. No URL
    # -> services.alert_service is a no-op (set on production only).
    alert_webhook_url: Optional[SecretStr] = None
    alert_webhook_format: str = "discord"  # "discord" (embeds) or "slack" ({"text": ...})
    alerts_enabled: bool = True
    # cron_failure_streak fires when a cron job's consecutive failures reach its
    # threshold (JSON in the env var; unknown jobs use the default).
    alert_cron_streak_thresholds: dict[str, int] = {
        "live-stats": 3,
        "pre-game": 2,
        "post-game": 2,
        "playoffs": 2,
        "schedule-sync": 1,
        "deploy": 1,
    }
    alert_cron_streak_default_threshold: int = 2

    # The private uvicorn process (main.py) as seen from the public one (main_public.py).
    # uvicorn binds `::` IPv6-only (asyncio sets IPV6_V6ONLY), hence the [::1] loopback.
    private_port: int = 8001
    private_health_url: Optional[str] = None  # derived from private_port when unset

    # Envelope-encryption keys for stored provider credentials, as
    # "1:<fernet-key>,2:<fernet-key>" (newest last). Must match the backend's
    # value -- both services decrypt the same rows. Empty disables the store and
    # falls back to the legacy plaintext column. See core/crypto.py.
    credential_keys: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is a valid Python logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        upper_v = v.upper()
        if upper_v not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return upper_v

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        """Validate log format is either json or console."""
        lower_v = v.lower()
        if lower_v not in {"json", "console"}:
            raise ValueError("log_format must be 'json' or 'console'")
        return lower_v

    @field_validator("alert_webhook_format")
    @classmethod
    def validate_alert_webhook_format(cls, v: str) -> str:
        lower_v = v.lower()
        if lower_v not in {"discord", "slack"}:
            raise ValueError("alert_webhook_format must be 'discord' or 'slack'")
        return lower_v

    @model_validator(mode="after")
    def require_private_backend_on_railway(self) -> "Settings":
        """A deployed data-platform must reach the backend over Railway private networking only.

        Mirrors the backend's `require_private_sqlmate_on_railway`: the pipeline
        token travels in this call, so it never leaves the private network.
        """
        if not self.railway_environment_name or not self.backend_internal_url:
            return self

        parsed = urlsplit(self.backend_internal_url)
        hostname = parsed.hostname or ""
        if parsed.scheme != "http" or not hostname.endswith(".railway.internal"):
            raise ValueError(
                "BACKEND_INTERNAL_URL must use an http://*.railway.internal private domain on Railway"
            )
        return self

    @model_validator(mode="after")
    def derive_season(self) -> "Settings":
        """NBA_SEASON defaults to today's season; ESPN_YEAR to the season's end year."""
        if not self.nba_season:
            self.nba_season = season_key()
        validate_season(self.nba_season)
        if not self.espn_year:
            self.espn_year = espn_year_for(self.nba_season)
        return self

    @model_validator(mode="after")
    def derive_sentry_environment(self) -> "Settings":
        if not self.sentry_environment:
            self.sentry_environment = self.environment
        if not self.private_health_url:
            self.private_health_url = f"http://[::1]:{self.private_port}/health"
        return self

    @property
    def environment(self) -> str:
        """Deployment environment name: Railway's, else "development"."""
        return self.railway_environment_name or "development"

    @property
    def release(self) -> Optional[str]:
        """The deployed build's identifier (APP_VERSION, else the Railway commit SHA); None locally."""
        return (self.app_version or "").strip() or (self.railway_git_commit_sha or "").strip() or None

    @property
    def version(self) -> str:
        """Short build identifier for /health, logs and alert footers ("dev" outside a deploy)."""
        return (self.release or "")[:7] or "dev"


def get_settings() -> Settings:
    """
    Get application settings.

    This function creates a new Settings instance each time,
    allowing for testing with different configurations.
    For production use, consider caching with functools.lru_cache.
    """
    return Settings()


# Default settings instance for convenience
# Import this for quick access: from core.settings import settings
settings = Settings()
