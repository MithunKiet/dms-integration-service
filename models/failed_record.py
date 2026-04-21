"""Data model for a record that failed to sync and is awaiting retry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class FailedRecord:
    """Persists information about a record that could not be synchronised.

    Attributes:
        id: Database primary key (``None`` before the record is persisted).
        job_name: Name of the job that encountered the failure.
        source_id: Identifier of the record in the source system.
        source_table: Source table or entity name.
        error_message: Description of the error that caused the failure.
        raw_data: Optional serialised snapshot of the source record.
        retry_count: Number of times this record has been retried.
        created_at: UTC timestamp when the failure was first recorded.
        last_retried_at: UTC timestamp of the most recent retry attempt.
        resolved: ``True`` once the record has been processed successfully.
    """

    id: Optional[int]
    job_name: str
    source_id: str
    source_table: str
    error_message: str
    raw_data: Optional[str] = None
    retry_count: int = 0
    created_at: Optional[datetime] = None
    last_retried_at: Optional[datetime] = None
    resolved: bool = False
