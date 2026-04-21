"""Notification service for alerting on critical failures."""
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class NotificationService:
    """Sends operational alerts for job failures and health degradation.

    The current implementation is log-based, making it easy to extend later
    with email, Slack, PagerDuty, or any other notification backend.
    """

    def notify_job_failure(
        self,
        job_name: str,
        error: str,
        log_id: Optional[int] = None,
    ) -> None:
        """Emit a critical-level alert for a job failure.

        Args:
            job_name: Name of the job that failed.
            error: Human-readable error description.
            log_id: Associated execution log ID, if available.
        """
        logger.critical(
            "JOB FAILURE ALERT: job='%s' error='%s' log_id=%s",
            job_name,
            error,
            log_id,
        )

    def notify_health_degraded(self, component: str, message: str) -> None:
        """Emit a warning-level alert for a degraded health component.

        Args:
            component: Name of the degraded component.
            message: Description of the degradation.
        """
        logger.warning(
            "HEALTH ALERT: component='%s' message='%s'",
            component,
            message,
        )
