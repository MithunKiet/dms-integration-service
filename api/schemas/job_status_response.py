"""Job status response schema."""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class JobLogResponse(BaseModel):
    """A single job execution log entry."""

    log_id: int
    job_name: str
    status: str
    run_type: str
    records_read: int
    records_processed: int
    records_failed: int
    started_at: Optional[datetime]
    ended_at: Optional[datetime]
    error_message: Optional[str]


class JobStatusResponse(BaseModel):
    """Job metadata together with recent execution history."""

    job_name: str
    is_active: bool
    description: Optional[str]
    recent_logs: List[JobLogResponse] = []
