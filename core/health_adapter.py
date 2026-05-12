"""Adapter that binds HealthChecker dependencies so it can be called with no args."""
from __future__ import annotations

from typing import Any, Optional

from core.db import DatabaseManager
from core.health import HealthChecker
from models.health_status import SystemHealth


class BoundHealthChecker:
    """Wraps :class:`~core.health.HealthChecker` with pre-bound dependencies.

    The FastAPI health router calls ``get_system_health()`` with no arguments.
    This adapter captures ``db_manager``, ``scheduler``, and optionally
    ``queue_repo`` at construction time so that the router call works transparently.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        scheduler: Any,
        queue_repo: Optional[Any] = None,
        include_integration_db: bool = True,
    ) -> None:
        self._checker = HealthChecker()
        self._db_manager = db_manager
        self._scheduler = scheduler
        self._queue_repo = queue_repo
        self._include_integration_db = include_integration_db

    def get_system_health(self) -> SystemHealth:
        """Return aggregated system health with no arguments required."""
        return self._checker.get_system_health(
            db_manager=self._db_manager,
            scheduler=self._scheduler,
            queue_repo=self._queue_repo,
            include_integration_db=self._include_integration_db,
        )

