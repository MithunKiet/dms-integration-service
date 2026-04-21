"""Data model for the result produced by a single job execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from models.enums import JobStatus


@dataclass
class JobResult:
    """Captures the outcome and statistics of a job run.

    Attributes:
        job_id: Unique identifier for this particular execution (UUID).
        job_name: Human-readable / registered name of the job.
        status: Final :class:`~models.enums.JobStatus` of the run.
        started_at: UTC timestamp when the job began.
        ended_at: UTC timestamp when the job finished (``None`` if still running).
        records_read: Number of source records fetched.
        records_processed: Number of records successfully transformed and written.
        records_failed: Number of records that could not be processed.
        error_message: Top-level error description when ``status`` is FAILED.
        checkpoint_value: Last successfully processed cursor / watermark value.
        run_type: How the job was triggered (e.g. ``"scheduled"``).
        metadata: Arbitrary extra data for debugging or auditing.
    """

    job_id: str
    job_name: str
    status: JobStatus
    started_at: datetime
    ended_at: Optional[datetime] = None
    records_read: int = 0
    records_processed: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    checkpoint_value: Optional[str] = None
    run_type: str = "scheduled"
    metadata: dict = field(default_factory=dict)
