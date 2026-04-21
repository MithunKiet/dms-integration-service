"""Enumeration types used across the DMS Integration Service."""

from __future__ import annotations

from enum import Enum


class JobStatus(str, Enum):
    """Possible lifecycle states for an integration job run."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class QueueStatus(str, Enum):
    """Possible states for an item in the on-demand job queue."""

    PENDING = "pending"
    PICKED = "picked"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SyncDirection(str, Enum):
    """Direction of a data synchronisation operation."""

    HMIS_TO_DMS = "hmis_to_dms"
    DMS_TO_HMIS = "dms_to_hmis"


class RunType(str, Enum):
    """How a job was triggered."""

    SCHEDULED = "scheduled"
    ON_DEMAND = "on_demand"
    RETRY = "retry"
    RECONCILIATION = "reconciliation"


class HealthStatus(str, Enum):
    """Operational health level of a component or the overall system."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class DbType(str, Enum):
    """Identifies which database a connection targets."""

    HMIS = "hmis"
    DMS = "dms"
    INTEGRATION = "integration"
