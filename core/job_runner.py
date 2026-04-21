"""Reusable job execution pipeline with checkpointing, locking, and audit logging."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Callable, Optional

from core.utils import utc_now
from models.enums import JobStatus, RunType
from models.job_result import JobResult

if TYPE_CHECKING:
    from services.audit_service import AuditService
    from services.checkpoint_service import CheckpointService
    from services.notification_service import NotificationService
    from repositories.integration.lock_repository import LockRepository

logger = logging.getLogger(__name__)


class JobRunner:
    """Provides a reusable execution pipeline for integration sync jobs.

    Pipeline steps:

    1. Acquire lock (prevent duplicate execution).
    2. Start audit log.
    3. Read checkpoint.
    4. Execute job function.
    5. Update checkpoint on success.
    6. Finish audit log.
    7. Release lock.

    Args:
        audit_service: Service for writing job execution logs.
        checkpoint_service: Service for reading and writing sync checkpoints.
        notification_service: Service for emitting failure alerts.
        lock_repo: Repository used to acquire and release job locks.
    """

    def __init__(
        self,
        audit_service: "AuditService",
        checkpoint_service: "CheckpointService",
        notification_service: "NotificationService",
        lock_repo: "LockRepository",
    ) -> None:
        self._audit = audit_service
        self._checkpoint = checkpoint_service
        self._notify = notification_service
        self._lock_repo = lock_repo

    def run(
        self,
        job_name: str,
        job_fn: Callable[["JobContext"], JobResult],
        run_type: str = RunType.SCHEDULED,
        queue_id: Optional[int] = None,
        lock_timeout_minutes: int = 60,
    ) -> JobResult:
        """Execute a job through the standard pipeline.

        Args:
            job_name: Unique job identifier.
            job_fn: Callable that accepts a :class:`JobContext` and returns a
                :class:`~models.job_result.JobResult`.
            run_type: How the job was triggered (see :class:`~models.enums.RunType`).
            queue_id: Associated queue item ID if triggered on-demand.
            lock_timeout_minutes: Lock expiry timeout in minutes.

        Returns:
            A :class:`~models.job_result.JobResult` describing the execution outcome.
        """
        started_at = utc_now()
        log_id: Optional[int] = None
        lock_acquired = False

        try:
            # Step 1: Acquire lock
            if not self._lock_repo.acquire(job_name, lock_timeout_minutes):
                logger.warning("Job '%s' is already running, skipping.", job_name)
                return JobResult(
                    job_id=job_name,
                    job_name=job_name,
                    status=JobStatus.SKIPPED,
                    started_at=started_at,
                    ended_at=utc_now(),
                    run_type=run_type,
                )
            lock_acquired = True

            # Step 2: Start audit log
            log_id = self._audit.start_job_log(job_name, run_type, queue_id)

            # Step 3: Read checkpoint
            checkpoint = self._checkpoint.get_checkpoint(job_name)

            # Step 4: Execute job
            context = JobContext(
                job_name=job_name,
                log_id=log_id,
                run_type=run_type,
                queue_id=queue_id,
                checkpoint=checkpoint,
            )
            result = job_fn(context)
            result.started_at = started_at
            result.ended_at = utc_now()

            # Step 5: Update checkpoint on success
            if result.status == JobStatus.COMPLETED and result.checkpoint_value:
                self._checkpoint.update_checkpoint(job_name, result.checkpoint_value)

            # Step 6: Finish audit log
            self._audit.finish_job_log(
                log_id,
                result.status.value,
                result.records_read,
                result.records_processed,
                result.records_failed,
                result.error_message,
            )

            return result

        except Exception as e:
            logger.exception("Unhandled error in job '%s': %s", job_name, e)
            if log_id is not None:
                try:
                    self._audit.finish_job_log(
                        log_id,
                        JobStatus.FAILED.value,
                        error_message=str(e)[:4000],
                    )
                except Exception:
                    pass
            self._notify.notify_job_failure(job_name, str(e), log_id)
            return JobResult(
                job_id=job_name,
                job_name=job_name,
                status=JobStatus.FAILED,
                started_at=started_at,
                ended_at=utc_now(),
                error_message=str(e)[:4000],
                run_type=run_type,
            )

        finally:
            if lock_acquired:
                try:
                    self._lock_repo.release(job_name)
                except Exception as e:
                    logger.error(
                        "Failed to release lock for job '%s': %s", job_name, e
                    )


class JobContext:
    """Context passed to each job function during execution.

    Attributes:
        job_name: Unique identifier for the running job.
        log_id: Primary key of the active audit log entry.
        run_type: How the job was triggered.
        queue_id: Associated queue item ID for on-demand runs.
        checkpoint: Current :class:`~models.checkpoint.Checkpoint` for the job.
    """

    def __init__(
        self,
        job_name: str,
        log_id: int,
        run_type: str,
        queue_id: Optional[int],
        checkpoint: Any,
    ) -> None:
        self.job_name = job_name
        self.log_id = log_id
        self.run_type = run_type
        self.queue_id = queue_id
        self.checkpoint = checkpoint
