"""System and component health-checking utilities."""

from __future__ import annotations

import logging
from typing import Any, Optional

from core.db import DatabaseManager
from core.utils import utc_now
from models.enums import DbType, HealthStatus
from models.health_status import ComponentHealth, SystemHealth

logger = logging.getLogger(__name__)

_HEALTH_QUERY = "SELECT 1"


class HealthChecker:
    """Provides health-check methods for each observable system component.

    All check methods return a :class:`~models.health_status.ComponentHealth`
    instance and never raise; errors are captured as UNHEALTHY status.
    """

    # ------------------------------------------------------------------
    # Individual component checks
    # ------------------------------------------------------------------

    def check_db(self, db_manager: DatabaseManager, db_type: DbType) -> ComponentHealth:
        """Probe a database connection with a lightweight query.

        Args:
            db_manager: The :class:`~core.db.DatabaseManager` to use.
            db_type: Which database to test.

        Returns:
            A :class:`~models.health_status.ComponentHealth` describing the result.
        """
        name = f"{db_type.value}_db"
        now = utc_now()
        try:
            ok = db_manager.test_connection(db_type)
            if ok:
                return ComponentHealth(
                    name=name,
                    status=HealthStatus.HEALTHY,
                    message="Connection successful.",
                    checked_at=now,
                )
            return ComponentHealth(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message="test_connection returned False.",
                checked_at=now,
            )
        except Exception as exc:
            logger.warning("Health check failed for %s: %s", name, exc)
            return ComponentHealth(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=str(exc),
                checked_at=now,
            )

    def check_scheduler(self, scheduler: Any) -> ComponentHealth:
        """Check whether the APScheduler instance is running.

        Args:
            scheduler: An APScheduler ``BackgroundScheduler`` (or compatible) instance.

        Returns:
            A :class:`~models.health_status.ComponentHealth` describing the result.
        """
        name = "scheduler"
        now = utc_now()
        try:
            running: bool = getattr(scheduler, "running", False)
            if running:
                return ComponentHealth(
                    name=name,
                    status=HealthStatus.HEALTHY,
                    message="Scheduler is running.",
                    checked_at=now,
                )
            return ComponentHealth(
                name=name,
                status=HealthStatus.DEGRADED,
                message="Scheduler is not running.",
                checked_at=now,
            )
        except Exception as exc:
            logger.warning("Scheduler health check error: %s", exc)
            return ComponentHealth(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=str(exc),
                checked_at=now,
            )

    def check_queue(self, queue_repo: Any) -> ComponentHealth:
        """Check the on-demand job queue component.

        Delegates to ``queue_repo.is_healthy()`` if the method exists,
        otherwise returns HEALTHY as a safe default.

        Args:
            queue_repo: Repository object that exposes an optional
                ``is_healthy() -> bool`` method.

        Returns:
            A :class:`~models.health_status.ComponentHealth` describing the result.
        """
        name = "queue"
        now = utc_now()
        try:
            if hasattr(queue_repo, "is_healthy"):
                ok: bool = queue_repo.is_healthy()
                status = HealthStatus.HEALTHY if ok else HealthStatus.DEGRADED
                message = "Queue is healthy." if ok else "Queue reported unhealthy."
            else:
                status = HealthStatus.HEALTHY
                message = "Queue check not implemented; assuming healthy."
            return ComponentHealth(
                name=name, status=status, message=message, checked_at=now
            )
        except Exception as exc:
            logger.warning("Queue health check error: %s", exc)
            return ComponentHealth(
                name=name,
                status=HealthStatus.UNHEALTHY,
                message=str(exc),
                checked_at=now,
            )

    # ------------------------------------------------------------------
    # Aggregated system health
    # ------------------------------------------------------------------

    def get_system_health(
        self,
        db_manager: DatabaseManager,
        scheduler: Any,
        queue_repo: Optional[Any] = None,
    ) -> SystemHealth:
        """Aggregate health across all components into a :class:`~models.health_status.SystemHealth`.

        The overall status is determined by the worst individual component:

        * If **any** component is UNHEALTHY → overall is UNHEALTHY.
        * Else if **any** component is DEGRADED → overall is DEGRADED.
        * Else → overall is HEALTHY.

        Args:
            db_manager: Database manager used to probe each database.
            scheduler: The running scheduler instance.
            queue_repo: Optional queue repository to probe.

        Returns:
            A :class:`~models.health_status.SystemHealth` with all component
            results and the aggregated overall status.
        """
        components: dict[str, ComponentHealth] = {}

        for db_type in (DbType.HMIS, DbType.DMS, DbType.INTEGRATION):
            comp = self.check_db(db_manager, db_type)
            components[comp.name] = comp

        scheduler_health = self.check_scheduler(scheduler)
        components[scheduler_health.name] = scheduler_health

        if queue_repo is not None:
            queue_health = self.check_queue(queue_repo)
            components[queue_health.name] = queue_health

        overall = _aggregate_status(list(components.values()))

        return SystemHealth(
            overall_status=overall,
            components=components,
            checked_at=utc_now(),
        )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _aggregate_status(components: list[ComponentHealth]) -> HealthStatus:
    """Compute the worst-case :class:`~models.enums.HealthStatus` across *components*."""
    statuses = {c.status for c in components}
    if HealthStatus.UNHEALTHY in statuses:
        return HealthStatus.UNHEALTHY
    if HealthStatus.DEGRADED in statuses:
        return HealthStatus.DEGRADED
    return HealthStatus.HEALTHY
