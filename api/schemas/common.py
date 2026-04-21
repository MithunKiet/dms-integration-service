"""Common API schema types."""
from typing import Optional

from pydantic import BaseModel


class BaseResponse(BaseModel):
    """Base response envelope shared by all API responses."""

    success: bool = True
    message: Optional[str] = None
