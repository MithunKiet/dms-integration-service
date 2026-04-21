"""Data model for an API request audit log entry."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class ApiAuditLog:
    """Records metadata about a single inbound API request for auditing.

    Attributes:
        id: Database primary key (``None`` before the record is persisted).
        client_id: Identifier of the API client that issued the request.
        endpoint_path: HTTP path of the called endpoint.
        method: HTTP method (e.g. ``"GET"``, ``"POST"``).
        status_code: HTTP response status code returned to the caller.
        remote_ip: IP address of the client (may be ``None`` if not resolvable).
        request_id: Correlation ID injected into the request context.
        response_time_ms: Wall-clock response time in milliseconds.
        created_at: UTC timestamp when the request was received.
        error_message: Optional error detail when the request resulted in an error.
        is_authorized: ``False`` when the request was rejected due to auth failure.
    """

    id: Optional[int]
    client_id: str
    endpoint_path: str
    method: str
    status_code: int
    remote_ip: Optional[str]
    request_id: Optional[str]
    response_time_ms: Optional[int]
    created_at: datetime
    error_message: Optional[str] = None
    is_authorized: bool = True
