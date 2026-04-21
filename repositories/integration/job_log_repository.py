"""Repository for IntegrationJobLogs table operations."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_INTEGRATION_JOB_LOGS

logger = logging.getLogger(__name__)


class JobLogRepository(BaseRepository):
    """Provides operations against the ``IntegrationJobLogs`` table."""

    def insert_log(
        self,
        job_name: str,
        run_type: str,
        queue_id: Optional[int] = None,
    ) -> int:
        """Insert a new job-log row and return its generated LogId.

        Args:
            job_name: Name of the executing job.
            run_type: How the job was triggered (e.g. 'scheduled', 'on_demand').
            queue_id: Associated queue item, if triggered on-demand.

        Returns:
            The auto-generated LogId of the new row.
        """
        sql = f"""
            INSERT INTO {TABLE_INTEGRATION_JOB_LOGS}
                (JobName, RunType, QueueId, Status, StartedAt)
            VALUES (?, ?, ?, 'running', GETUTCDATE());
            SELECT SCOPE_IDENTITY() AS LogId;
        """
        log_id = self.execute_scalar(sql, (job_name, run_type, queue_id))
        logger.debug("Inserted job log %s for job '%s'.", log_id, job_name)
        return int(log_id)

    def update_log(
        self,
        log_id: int,
        status: str,
        records_read: int = 0,
        records_processed: int = 0,
        records_failed: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Update a job-log row with the final execution outcome.

        Args:
            log_id: Primary key of the log row to update.
            status: Final job status string (e.g. 'completed', 'failed').
            records_read: Number of source records fetched.
            records_processed: Number of records successfully written.
            records_failed: Number of records that could not be processed.
            error_message: Top-level error message when status is 'failed'.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOB_LOGS}
               SET Status           = ?,
                   RecordsRead      = ?,
                   RecordsProcessed = ?,
                   RecordsFailed    = ?,
                   ErrorMessage     = ?,
                   EndedAt          = GETUTCDATE()
             WHERE LogId            = ?
        """
        err_str = error_message[:4000] if error_message else None
        self.execute_non_query(
            sql,
            (status, records_read, records_processed, records_failed, err_str, log_id),
        )

    def get_log(self, log_id: int) -> Optional[dict]:
        """Fetch a single log row by its primary key.

        Args:
            log_id: Primary key of the log entry.

        Returns:
            A row dict, or ``None`` if not found.
        """
        sql = f"SELECT * FROM {TABLE_INTEGRATION_JOB_LOGS} WHERE LogId = ?"
        rows = self.execute_query(sql, (log_id,))
        if rows:
            return self.row_to_dict(rows[0])
        return None

    def get_logs_for_job(self, job_name: str, limit: int = 20) -> List[dict]:
        """Return recent log entries for a specific job, newest first.

        Args:
            job_name: The job whose logs should be fetched.
            limit: Maximum number of rows to return.

        Returns:
            A list of row dicts ordered by StartedAt descending.
        """
        sql = f"""
            SELECT TOP (?) *
              FROM {TABLE_INTEGRATION_JOB_LOGS}
             WHERE JobName  = ?
             ORDER BY StartedAt DESC
        """
        rows = self.execute_query(sql, (limit, job_name))
        return self.rows_to_dicts(rows)
