"""General-purpose utility functions for the DMS Integration Service."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def utc_now() -> datetime:
    """Return the current UTC datetime with timezone information attached.

    Returns:
        A timezone-aware :class:`datetime` set to the current UTC time.
    """
    return datetime.now(tz=timezone.utc)


def safe_str(value: Any, max_len: int = 4000) -> str:
    """Convert *value* to a string, capping the result at *max_len* characters.

    Args:
        value: Any Python object to convert.
        max_len: Maximum length of the returned string.

    Returns:
        A string representation of *value* truncated to *max_len* characters.
    """
    text = str(value) if value is not None else ""
    return text[:max_len]


def mask_secret(value: str, visible_chars: int = 4) -> str:
    """Return a masked version of *value*, showing only the first few characters.

    Args:
        value: The secret string to mask (e.g. an API key).
        visible_chars: Number of leading characters to leave visible.

    Returns:
        A string like ``"abcd****"`` with the tail replaced by asterisks.
        If *value* is shorter than or equal to *visible_chars*, the whole
        string is replaced by asterisks.
    """
    if not value:
        return ""
    if len(value) <= visible_chars:
        return "*" * len(value)
    return value[:visible_chars] + "*" * (len(value) - visible_chars)


def truncate(value: str, max_len: int) -> str:
    """Truncate *value* to *max_len* characters.

    Args:
        value: Input string.
        max_len: Maximum allowed length.

    Returns:
        The original string if it fits, otherwise a truncated copy.
    """
    return value[:max_len]


def parse_json_safe(value: str) -> Optional[dict]:
    """Attempt to parse *value* as JSON, returning ``None`` on failure.

    Args:
        value: A JSON-encoded string.

    Returns:
        The parsed dictionary, or ``None`` if parsing fails or the result is
        not a :class:`dict`.
    """
    if not value:
        return None
    try:
        result = json.loads(value)
        return result if isinstance(result, dict) else None
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def generate_request_id() -> str:
    """Generate a random UUID4 string suitable for use as a request correlation ID.

    Returns:
        A hyphenated UUID4 string (e.g. ``"550e8400-e29b-41d4-a716-446655440000"``).
    """
    return str(uuid.uuid4())
