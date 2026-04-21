"""Application settings loaded from environment variables via pydantic-settings."""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralised application configuration sourced from environment variables or a .env file."""

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    # Application
    app_name: str = "DMS Integration Service"
    app_env: str = "development"
    debug: bool = False
    log_level: str = "INFO"
    log_dir: str = "logs"

    # Database connections
    hmis_connection: str = ""
    dms_connection: str = ""
    integration_connection: str = ""

    # API settings
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 1

    # Scheduler
    scheduler_timezone: str = "UTC"
    job_max_instances: int = 1
    job_coalesce: bool = True
    job_misfire_grace_time: int = 300

    # Queue poller
    queue_poll_interval: int = 10
    queue_max_concurrent: int = 3

    # Retry
    retry_max_attempts: int = 3
    retry_backoff_seconds: int = 30

    # Security
    require_https: bool = False
    api_keys_enabled: bool = True

    @classmethod
    def get_settings(cls) -> "Settings":
        """Return a cached singleton Settings instance."""
        return _get_settings()


@lru_cache(maxsize=1)
def _get_settings() -> Settings:
    """Inner cached factory used by :meth:`Settings.get_settings`."""
    return Settings()
