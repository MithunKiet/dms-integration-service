"""Repository for IntegrationFailedRecords table operations."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_INTEGRATION_FAILED_RECORDS

logger = logging.getLogger(__name__)


class FailedRecordRepository(BaseRepository):
    """Provides operations against the ``IntegrationFailedRecords`` table."""

    def save_failed_record(
        self,
        job_name: str,
        source_id: str,
        source_table: str,
        error_message: str,
        raw_data: Optional[str] = None,
    ) -> int:
        """Persist a failed record for later retry or investigation.

        Args:
            job_name: Name of the job that failed to process this record.
            source_id: Primary-key value of the record in the source system.
            source_table: Source table or entity name.
            error_message: Human-readable description of the failure.
            raw_data: Optional JSON dump of the full source record.

        Returns:
            The auto-generated RecordId of the new row.
        """
        sql = f"""
            INSERT INTO {TABLE_INTEGRATION_FAILED_RECORDS}
                (JobName, SourceId, SourceTable, ErrorMessage, RawData,
                 IsResolved, RetryCount, CreatedAt)
            VALUES (?, ?, ?, ?, ?, 0, 0, GETUTCDATE());
            SELECT SCOPE_IDENTITY() AS RecordId;
        """
        record_id = self.execute_scalar(
            sql,
            (job_name, source_id, source_table, error_message[:4000], raw_data),
        )
        logger.debug(
            "Saved failed record %s for job '%s', source_id='%s'.",
            record_id,
            job_name,
            source_id,
        )
        return int(record_id)

    def get_unresolved_by_job(self, job_name: str, limit: int = 100) -> List[dict]:
        """Return unresolved failed records for a given job.

        Args:
            job_name: The job whose failed records should be fetched.
            limit: Maximum number of rows to return.

        Returns:
            A list of row dicts ordered by CreatedAt ascending (oldest first).
        """
        sql = f"""
            SELECT TOP (?) *
              FROM {TABLE_INTEGRATION_FAILED_RECORDS}
             WHERE JobName    = ?
               AND IsResolved = 0
             ORDER BY CreatedAt ASC
        """
        rows = self.execute_query(sql, (limit, job_name))
        return self.rows_to_dicts(rows)

    def mark_resolved(self, record_id: int) -> None:
        """Mark a failed record as resolved.

        Args:
            record_id: Primary key of the failed-record row.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_FAILED_RECORDS}
               SET IsResolved  = 1,
                   ResolvedAt  = GETUTCDATE()
             WHERE RecordId    = ?
        """
        self.execute_non_query(sql, (record_id,))
        logger.debug("Marked failed record %d as resolved.", record_id)

    def increment_retry(self, record_id: int) -> None:
        """Increment the retry counter for a failed record.

        Args:
            record_id: Primary key of the failed-record row.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_FAILED_RECORDS}
               SET RetryCount  = RetryCount + 1,
                   LastRetriedAt = GETUTCDATE()
             WHERE RecordId    = ?
        """
        self.execute_non_query(sql, (record_id,))
        logger.debug("Incremented retry count for failed record %d.", record_id)
