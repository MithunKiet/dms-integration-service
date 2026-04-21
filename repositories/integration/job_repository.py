"""Repository for IntegrationJobs table operations."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_INTEGRATION_JOBS

logger = logging.getLogger(__name__)


class JobRepository(BaseRepository):
    """Provides CRUD operations against the ``IntegrationJobs`` table."""

    def get_job_by_name(self, job_name: str) -> Optional[dict]:
        """Fetch a single job record by its unique name.

        Args:
            job_name: The registered name of the integration job.

        Returns:
            A dict of column values, or ``None`` if not found.
        """
        sql = f"SELECT * FROM {TABLE_INTEGRATION_JOBS} WHERE JobName = ?"
        rows = self.execute_query(sql, (job_name,))
        if rows:
            return self.row_to_dict(rows[0])
        return None

    def get_all_jobs(self) -> List[dict]:
        """Return all rows from the IntegrationJobs table.

        Returns:
            A list of dicts, one per job record.
        """
        sql = f"SELECT * FROM {TABLE_INTEGRATION_JOBS} ORDER BY JobName"
        rows = self.execute_query(sql)
        return self.rows_to_dicts(rows)

    def upsert_job(
        self,
        job_name: str,
        description: str,
        is_active: bool = True,
    ) -> None:
        """Insert or update a job record.

        Uses a SQL Server MERGE to avoid duplicate-key errors on re-registration.

        Args:
            job_name: Unique identifier for the job.
            description: Human-readable description of the job.
            is_active: Whether the job is enabled.
        """
        sql = f"""
            MERGE INTO {TABLE_INTEGRATION_JOBS} AS target
            USING (SELECT ? AS JobName) AS source
              ON target.JobName = source.JobName
            WHEN MATCHED THEN
                UPDATE SET
                    Description = ?,
                    IsActive    = ?,
                    UpdatedAt   = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (JobName, Description, IsActive, CreatedAt, UpdatedAt)
                VALUES (?, ?, ?, GETUTCDATE(), GETUTCDATE());
        """
        active_flag = 1 if is_active else 0
        self.execute_non_query(
            sql,
            (job_name, description, active_flag, job_name, description, active_flag),
        )
        logger.debug("Upserted job '%s'.", job_name)

    def set_job_active(self, job_name: str, is_active: bool) -> None:
        """Enable or disable a job by name.

        Args:
            job_name: The job to update.
            is_active: ``True`` to enable, ``False`` to disable.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOBS}
               SET IsActive  = ?,
                   UpdatedAt = GETUTCDATE()
             WHERE JobName   = ?
        """
        active_flag = 1 if is_active else 0
        self.execute_non_query(sql, (active_flag, job_name))
        logger.debug("Job '%s' active=%s.", job_name, is_active)
