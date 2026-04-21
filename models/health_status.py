"""Data models representing the health state of system components."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional

from models.enums import HealthStatus


@dataclass
class ComponentHealth:
    """Health status for a single observable component.

    Attributes:
        name: Human-readable component name (e.g. ``"hmis_db"``).
        status: Current :class:`~models.enums.HealthStatus`.
        message: Optional descriptive message (e.g. error detail).
        checked_at: UTC timestamp when the check was performed.
    """

    name: str
    status: HealthStatus
    message: Optional[str] = None
    checked_at: Optional[datetime] = None


@dataclass
class SystemHealth:
    """Aggregated health state of the entire service.

    Attributes:
        overall_status: Worst-case aggregation of all component statuses.
        components: Map of component name to its :class:`ComponentHealth`.
        checked_at: UTC timestamp when the system-wide check was performed.
        version: Application version string.
    """

    overall_status: HealthStatus
    components: Dict[str, ComponentHealth] = field(default_factory=dict)
    checked_at: Optional[datetime] = None
    version: str = "1.0.0"
