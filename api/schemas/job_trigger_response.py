"""Job trigger response schema."""
from pydantic import BaseModel

from models.enums import QueueStatus


class JobTriggerResponse(BaseModel):
    """Response returned after a job has been successfully enqueued."""

    queue_id: int
    job_name: str
    status: QueueStatus
    message: str = "Job queued successfully"
