"""Queue item response schema."""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel

from models.enums import QueueStatus


class QueueItemResponse(BaseModel):
    """Serialisable representation of a single queue entry."""

    queue_id: int
    job_name: str
    status: QueueStatus
    priority: int
    requested_by: Optional[str]
    created_at: Optional[datetime]
    picked_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]


class QueueListResponse(BaseModel):
    """Paginated list of queue items."""

    items: List[QueueItemResponse]
    total: int
