"""Repository for IntegrationSyncState (checkpoint) table operations."""
from typing import Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_INTEGRATION_SYNC_STATE

logger = logging.getLogger(__name__)


class CheckpointRepository(BaseRepository):
    """Provides operations against the ``IntegrationSyncState`` table."""

    def get_checkpoint(self, job_name: str) -> Optional[dict]:
        """Retrieve the current checkpoint row for a job.

        Args:
            job_name: The job whose checkpoint should be fetched.

        Returns:
            A row dict with LastSyncValue, LastSyncAt, and ExtraState fields,
            or ``None`` if no checkpoint has been recorded yet.
        """
        sql = f"SELECT * FROM {TABLE_INTEGRATION_SYNC_STATE} WHERE JobName = ?"
        rows = self.execute_query(sql, (job_name,))
        if rows:
            return self.row_to_dict(rows[0])
        return None

    def upsert_checkpoint(
        self,
        job_name: str,
        last_sync_value: str,
        extra_state: Optional[str] = None,
    ) -> None:
        """Insert or update the checkpoint for a job.

        Uses a SQL Server MERGE so that the first call inserts and subsequent
        calls update without raising a duplicate-key error.

        Args:
            job_name: The job whose checkpoint should be persisted.
            last_sync_value: The latest cursor value (e.g. ISO timestamp or row ID).
            extra_state: Optional serialised JSON for any additional job state.
        """
        sql = f"""
            MERGE INTO {TABLE_INTEGRATION_SYNC_STATE} AS target
            USING (SELECT ? AS JobName) AS source
              ON target.JobName = source.JobName
            WHEN MATCHED THEN
                UPDATE SET
                    LastSyncValue = ?,
                    LastSyncAt    = GETUTCDATE(),
                    ExtraState    = ?
            WHEN NOT MATCHED THEN
                INSERT (JobName, LastSyncValue, LastSyncAt, ExtraState)
                VALUES (?, ?, GETUTCDATE(), ?);
        """
        self.execute_non_query(
            sql,
            (job_name, last_sync_value, extra_state, job_name, last_sync_value, extra_state),
        )
        logger.debug("Checkpoint upserted for job '%s': %s", job_name, last_sync_value)
