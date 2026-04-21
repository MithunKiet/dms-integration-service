"""Data model for an API client that is authorised to call the service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class ApiClient:
    """Represents a registered API consumer with its credentials and permissions.

    Attributes:
        client_id: Unique identifier for the client (e.g. ``"svc-erp"``).
        client_name: Human-readable display name.
        key_prefix: Short prefix embedded at the start of the raw API key.
        key_hash: bcrypt hash of the full raw API key stored for verification.
        is_active: Whether this client is currently allowed to authenticate.
        allowed_endpoints: List of endpoint path patterns the client may call.
            An entry of ``"*"`` grants access to all endpoints.
        rate_limit_per_minute: Maximum requests per minute for this client.
        allowed_ip_ranges: Optional comma-separated CIDR ranges for IP filtering.
        created_at: UTC timestamp when the client was registered.
        expires_at: Optional UTC expiry timestamp for the API key.
        last_used_at: UTC timestamp of the most recent successful request.
    """

    client_id: str
    client_name: str
    key_prefix: Optional[str]
    key_hash: str
    is_active: bool
    allowed_endpoints: List[str] = field(default_factory=list)
    rate_limit_per_minute: int = 60
    allowed_ip_ranges: Optional[str] = None
    created_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None
