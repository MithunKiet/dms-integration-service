"""API error response schema."""
from typing import Optional

from pydantic import BaseModel


class ApiErrorResponse(BaseModel):
    """Structured error payload returned for all 4xx/5xx responses."""

    success: bool = False
    error_code: str
    message: str
    detail: Optional[str] = None
