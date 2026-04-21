"""APScheduler management for recurring integration jobs."""
from __future__ import annotations

import logging
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED

from config.settings import Settings

logger = logging.getLogger(__name__)


class SchedulerManager:
    """Wraps an APScheduler :class:`BackgroundScheduler` with lifecycle management.

    Settings such as timezone, coalesce behaviour, and misfire grace time are
    read from the application :class:`~config.settings.Settings` singleton at
    construction time.
    """

    def __init__(self) -> None:
        settings = Settings.get_settings()
        self._scheduler = BackgroundScheduler(
            timezone=settings.scheduler_timezone,
            job_defaults={
                "coalesce": settings.job_coalesce,
                "max_instances": settings.job_max_instances,
                "misfire_grace_time": settings.job_misfire_grace_time,
            },
        )
        self._scheduler.add_listener(
            self._on_job_event,
            EVENT_JOB_ERROR | EVENT_JOB_EXECUTED | EVENT_JOB_MISSED,
        )

    def _on_job_event(self, event: Any) -> None:
        """Handle APScheduler job lifecycle events.

        Args:
            event: The APScheduler event object.
        """
        if hasattr(event, "exception") and event.exception:
            logger.error(
                "Scheduled job '%s' raised exception: %s",
                event.job_id,
                event.exception,
            )
        elif hasattr(event, "job_id"):
            logger.debug(
                "Scheduled job event: job_id='%s' code=%s",
                event.job_id,
                event.code,
            )

    def start(self) -> None:
        """Start the background scheduler if it is not already running."""
        if not self._scheduler.running:
            self._scheduler.start()
            logger.info("Scheduler started.")

    def stop(self, wait: bool = True) -> None:
        """Shut down the background scheduler.

        Args:
            wait: If ``True`` (default), wait for running jobs to complete
                before returning.
        """
        if self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Scheduler stopped.")

    @property
    def is_running(self) -> bool:
        """``True`` if the scheduler is currently active."""
        return self._scheduler.running

    def add_job(self, func: Any, job_id: str, trigger: Any, **kwargs: Any) -> None:
        """Register a job with the scheduler.

        Args:
            func: The callable to execute.
            job_id: Unique identifier for the job.
            trigger: An APScheduler trigger instance (cron, interval, etc.).
            **kwargs: Additional keyword arguments forwarded to
                :meth:`apscheduler.schedulers.base.BaseScheduler.add_job`.
        """
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            replace_existing=True,
            **kwargs,
        )
        logger.info("Job added to scheduler: %s", job_id)

    def remove_job(self, job_id: str) -> None:
        """Remove a job from the scheduler, ignoring errors if it does not exist.

        Args:
            job_id: Unique identifier of the job to remove.
        """
        try:
            self._scheduler.remove_job(job_id)
        except Exception:
            pass

    def get_jobs(self) -> list:
        """Return all currently scheduled jobs.

        Returns:
            A list of :class:`apscheduler.job.Job` instances.
        """
        return self._scheduler.get_jobs()

    def get_scheduler(self) -> BackgroundScheduler:
        """Return the underlying :class:`BackgroundScheduler` instance.

        Returns:
            The raw APScheduler scheduler for advanced use cases.
        """
        return self._scheduler
