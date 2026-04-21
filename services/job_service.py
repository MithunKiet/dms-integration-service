"""Service for reading job metadata and execution logs."""
from typing import Optional, List
import logging

from repositories.integration.job_repository import JobRepository
from repositories.integration.job_log_repository import JobLogRepository

logger = logging.getLogger(__name__)


class JobService:
    """Exposes job metadata and historical log data to API handlers."""

    def __init__(
        self,
        job_repo: JobRepository,
        log_repo: JobLogRepository,
    ) -> None:
        self._job_repo = job_repo
        self._log_repo = log_repo

    def get_job(self, job_name: str) -> Optional[dict]:
        """Fetch a single job record by name.

        Args:
            job_name: Unique registered name of the job.

        Returns:
            A row dict, or ``None`` if the job does not exist.
        """
        return self._job_repo.get_job_by_name(job_name)

    def get_all_jobs(self) -> List[dict]:
        """Return all registered job records.

        Returns:
            A list of row dicts ordered by JobName.
        """
        return self._job_repo.get_all_jobs()

    def get_job_logs(self, job_name: str, limit: int = 20) -> List[dict]:
        """Return recent execution log entries for a specific job.

        Args:
            job_name: Name of the job whose logs should be retrieved.
            limit: Maximum number of log entries to return.

        Returns:
            A list of row dicts ordered by StartedAt descending.
        """
        return self._log_repo.get_logs_for_job(job_name, limit)
