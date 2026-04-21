"""Job trigger request schema."""
from typing import Any, Dict, Optional

from pydantic import BaseModel, field_validator

from config.constants import ALL_JOB_NAMES


class JobTriggerRequest(BaseModel):
    """Payload accepted by the POST /api/jobs/trigger endpoint."""

    job_name: str
    payload: Optional[Dict[str, Any]] = None
    priority: int = 5

    @field_validator("job_name")
    @classmethod
    def validate_job_name(cls, v: str) -> str:
        """Ensure the requested job name is registered."""
        if v not in ALL_JOB_NAMES:
            raise ValueError(f"Unknown job_name '{v}'. Valid: {ALL_JOB_NAMES}")
        return v

    @field_validator("priority")
    @classmethod
    def validate_priority(cls, v: int) -> int:
        """Ensure priority is within the allowed 1-10 range."""
        if not 1 <= v <= 10:
            raise ValueError("priority must be between 1 and 10")
        return v
