"""Service for managing the IntegrationJobQueue."""
from typing import Optional, List
import json
import logging

from repositories.integration.queue_repository import QueueRepository
from models.queue_item import QueueItem
from models.enums import QueueStatus
from config.constants import ALL_JOB_NAMES
from core.exceptions import QueueError

logger = logging.getLogger(__name__)


class QueueService:
    """Manages enqueueing, polling, and status transitions for the job queue."""

    def __init__(self, queue_repo: QueueRepository) -> None:
        self._repo = queue_repo

    def enqueue(
        self,
        job_name: str,
        requested_by: str,
        payload: Optional[dict] = None,
        priority: int = 5,
    ) -> int:
        """Add a job execution request to the queue.

        Args:
            job_name: Registered name of the job to enqueue.
            requested_by: Identifier of the API client making the request.
            payload: Optional dict of job parameters; serialised to JSON.
            priority: Numeric priority (lower = higher priority).

        Returns:
            The QueueId of the newly created queue item.

        Raises:
            :class:`~core.exceptions.QueueError`: If *job_name* is not a known job.
        """
        if job_name not in ALL_JOB_NAMES:
            raise QueueError(f"Unknown job name: {job_name}")
        payload_str = json.dumps(payload) if payload else None
        queue_id = self._repo.insert_queue_item(job_name, requested_by, payload_str, priority)
        logger.info(
            "Job '%s' enqueued by '%s' with queue_id=%d", job_name, requested_by, queue_id
        )
        return queue_id

    def get_pending(self, limit: int = 10) -> List[QueueItem]:
        """Return pending queue items ready for dispatch.

        Args:
            limit: Maximum number of items to return.

        Returns:
            A list of :class:`~models.queue_item.QueueItem` instances.
        """
        rows = self._repo.get_pending_items(limit)
        items: List[QueueItem] = []
        for row in rows:
            items.append(
                QueueItem(
                    queue_id=row["QueueId"],
                    job_name=row["JobName"],
                    status=QueueStatus(row["Status"]),
                    created_at=row["CreatedAt"],
                    requested_by=row.get("RequestedBy"),
                    payload=json.loads(row["Payload"]) if row.get("Payload") else None,
                    priority=row.get("Priority", 5),
                )
            )
        return items

    def mark_running(self, queue_id: int) -> None:
        """Transition a queue item to the 'running' state.

        Args:
            queue_id: Primary key of the queue item.
        """
        self._repo.mark_running(queue_id)

    def mark_completed(self, queue_id: int) -> None:
        """Transition a queue item to the 'completed' state.

        Args:
            queue_id: Primary key of the queue item.
        """
        self._repo.mark_completed(queue_id)

    def mark_failed(self, queue_id: int, error: str) -> None:
        """Transition a queue item to the 'failed' state.

        Args:
            queue_id: Primary key of the queue item.
            error: Description of the failure.
        """
        self._repo.mark_failed(queue_id, error)

    def list_items(self, limit: int = 50) -> List[dict]:
        """Return recent queue items as raw dicts.

        Args:
            limit: Maximum number of items to return.

        Returns:
            A list of row dicts ordered by CreatedAt descending.
        """
        return self._repo.list_queue_items(limit)
