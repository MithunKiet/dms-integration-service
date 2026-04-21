"""Health check response schema."""
from typing import Dict, Optional

from pydantic import BaseModel

from models.enums import HealthStatus


class ComponentHealthResponse(BaseModel):
    """Health status for a single system component."""

    name: str
    status: HealthStatus
    message: Optional[str] = None


class HealthResponse(BaseModel):
    """Aggregate health status for the entire service."""

    overall_status: HealthStatus
    components: Dict[str, ComponentHealthResponse]
    version: str = "1.0.0"
