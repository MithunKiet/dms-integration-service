"""Service for writing job execution audit logs to the database."""
from typing import Optional
import logging

from repositories.integration.job_log_repository import JobLogRepository

logger = logging.getLogger(__name__)


class AuditService:
    """Writes job-lifecycle audit entries to the IntegrationJobLogs table."""

    def __init__(self, log_repo: JobLogRepository) -> None:
        self._repo = log_repo

    def start_job_log(
        self,
        job_name: str,
        run_type: str,
        queue_id: Optional[int] = None,
    ) -> int:
        """Create a new job-log entry and return its ID.

        Args:
            job_name: Name of the job starting execution.
            run_type: How the job was triggered (e.g. 'scheduled', 'on_demand').
            queue_id: Associated queue item ID for on-demand runs.

        Returns:
            The LogId of the created entry.
        """
        log_id = self._repo.insert_log(job_name, run_type, queue_id)
        logger.info(
            "Job log started: job='%s' run_type='%s' log_id=%d",
            job_name,
            run_type,
            log_id,
        )
        return log_id

    def finish_job_log(
        self,
        log_id: int,
        status: str,
        records_read: int = 0,
        records_processed: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Update a job-log entry with final execution metrics.

        Args:
            log_id: Primary key of the log entry to update.
            status: Final status string (e.g. 'completed', 'failed').
            records_read: Number of source records fetched.
            records_processed: Number of records successfully written.
            records_failed: Number of records that could not be processed.
            error_message: Top-level error when status is 'failed'.
        """
        self._repo.update_log(
            log_id,
            status,
            records_read,
            records_processed,
            records_failed,
            error_message,
        )
        logger.info(
            "Job log finished: log_id=%d status='%s' processed=%d failed=%d",
            log_id,
            status,
            records_processed,
            records_failed,
        )
