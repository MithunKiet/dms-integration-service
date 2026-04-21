"""Data model for an on-demand job queue item."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from models.enums import QueueStatus


@dataclass
class QueueItem:
    """Represents a single entry in the on-demand job execution queue.

    Attributes:
        queue_id: Auto-incremented primary key from the database.
        job_name: Name of the job to be executed.
        status: Current :class:`~models.enums.QueueStatus` of this item.
        created_at: UTC timestamp when the item was enqueued.
        requested_by: Identifier of the API client that created the request.
        payload: Optional JSON-serialisable parameters for the job.
        picked_at: UTC timestamp when a worker claimed this item.
        completed_at: UTC timestamp when processing finished.
        error_message: Error detail if status is FAILED.
        priority: Numeric priority; lower values are processed first.
    """

    queue_id: int
    job_name: str
    status: QueueStatus
    created_at: datetime
    requested_by: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None
    picked_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    priority: int = 5
