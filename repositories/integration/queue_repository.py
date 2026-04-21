"""Repository for IntegrationJobQueue table operations."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_INTEGRATION_JOB_QUEUE

logger = logging.getLogger(__name__)


class QueueRepository(BaseRepository):
    """Provides operations against the ``IntegrationJobQueue`` table."""

    def insert_queue_item(
        self,
        job_name: str,
        requested_by: str,
        payload: Optional[str] = None,
        priority: int = 5,
    ) -> int:
        """Insert a new pending queue item and return its generated QueueId.

        Args:
            job_name: Name of the job to enqueue.
            requested_by: Identifier of the API client making the request.
            payload: Optional JSON string of job parameters.
            priority: Numeric priority; lower values are processed first.

        Returns:
            The auto-generated QueueId of the new row.
        """
        sql = f"""
            INSERT INTO {TABLE_INTEGRATION_JOB_QUEUE}
                (JobName, RequestedBy, Payload, Priority, Status, CreatedAt)
            VALUES (?, ?, ?, ?, 'pending', GETUTCDATE());
            SELECT SCOPE_IDENTITY() AS QueueId;
        """
        queue_id = self.execute_scalar(sql, (job_name, requested_by, payload, priority))
        logger.debug("Inserted queue item %s for job '%s'.", queue_id, job_name)
        return int(queue_id)

    def get_pending_items(self, limit: int = 10) -> List[dict]:
        """Return up to *limit* pending queue items ordered by priority then age.

        Args:
            limit: Maximum number of rows to return.

        Returns:
            A list of row dicts with Status = 'pending'.
        """
        sql = f"""
            SELECT TOP (?) *
              FROM {TABLE_INTEGRATION_JOB_QUEUE}
             WHERE Status = 'pending'
             ORDER BY Priority ASC, CreatedAt ASC
        """
        rows = self.execute_query(sql, (limit,))
        return self.rows_to_dicts(rows)

    def mark_picked(self, queue_id: int) -> None:
        """Transition a queue item to the 'picked' state.

        Args:
            queue_id: Primary key of the queue row to update.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOB_QUEUE}
               SET Status   = 'picked',
                   PickedAt = GETUTCDATE()
             WHERE QueueId  = ?
        """
        self.execute_non_query(sql, (queue_id,))

    def mark_running(self, queue_id: int) -> None:
        """Transition a queue item to the 'running' state.

        Args:
            queue_id: Primary key of the queue row to update.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOB_QUEUE}
               SET Status    = 'running',
                   StartedAt = GETUTCDATE()
             WHERE QueueId   = ?
        """
        self.execute_non_query(sql, (queue_id,))

    def mark_completed(self, queue_id: int) -> None:
        """Transition a queue item to the 'completed' state.

        Args:
            queue_id: Primary key of the queue row to update.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOB_QUEUE}
               SET Status      = 'completed',
                   CompletedAt = GETUTCDATE()
             WHERE QueueId     = ?
        """
        self.execute_non_query(sql, (queue_id,))

    def mark_failed(self, queue_id: int, error: str) -> None:
        """Transition a queue item to the 'failed' state with an error message.

        Args:
            queue_id: Primary key of the queue row to update.
            error: Description of the failure (truncated to 4000 chars).
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOB_QUEUE}
               SET Status       = 'failed',
                   ErrorMessage = ?,
                   CompletedAt  = GETUTCDATE()
             WHERE QueueId      = ?
        """
        self.execute_non_query(sql, (error[:4000], queue_id))

    def get_queue_item(self, queue_id: int) -> Optional[dict]:
        """Fetch a single queue row by its primary key.

        Args:
            queue_id: Primary key of the queue item.

        Returns:
            A row dict, or ``None`` if not found.
        """
        sql = f"SELECT * FROM {TABLE_INTEGRATION_JOB_QUEUE} WHERE QueueId = ?"
        rows = self.execute_query(sql, (queue_id,))
        if rows:
            return self.row_to_dict(rows[0])
        return None

    def list_queue_items(self, limit: int = 50) -> List[dict]:
        """Return recent queue items, newest first.

        Args:
            limit: Maximum number of rows to return.

        Returns:
            A list of row dicts ordered by CreatedAt descending.
        """
        sql = f"""
            SELECT TOP (?) *
              FROM {TABLE_INTEGRATION_JOB_QUEUE}
             ORDER BY CreatedAt DESC
        """
        rows = self.execute_query(sql, (limit,))
        return self.rows_to_dicts(rows)

    def cancel_item(self, queue_id: int) -> None:
        """Cancel a pending queue item.

        Args:
            queue_id: Primary key of the queue item to cancel.
        """
        sql = f"""
            UPDATE {TABLE_INTEGRATION_JOB_QUEUE}
               SET Status      = 'cancelled',
                   CompletedAt = GETUTCDATE()
             WHERE QueueId     = ?
               AND Status      = 'pending'
        """
        self.execute_non_query(sql, (queue_id,))
        logger.debug("Cancelled queue item %d.", queue_id)
