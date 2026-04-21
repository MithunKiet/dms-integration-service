"""Data model for a job synchronisation checkpoint."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Checkpoint:
    """Stores the last-known sync position for an integration job.

    The ``last_sync_value`` is a generic string that can hold a timestamp,
    an integer sequence number, or any other cursor that the job uses to
    perform incremental data extraction.

    Attributes:
        job_name: Name of the job that owns this checkpoint.
        last_sync_value: Serialised cursor value (e.g. ISO timestamp, row ID).
        last_sync_at: UTC timestamp of the last successful sync.
        extra_state: Optional JSON string for any additional state the job needs.
    """

    job_name: str
    last_sync_value: Optional[str] = None
    last_sync_at: Optional[datetime] = None
    extra_state: Optional[str] = None
