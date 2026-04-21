"""FastAPI dependency functions for authentication and authorization."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Optional

from fastapi import Header, HTTPException, Request, status

from config.constants import HEADER_API_KEY, HEADER_CLIENT_ID
from core.exceptions import AuthenticationError, AuthorizationError
from models.api_client import ApiClient

if TYPE_CHECKING:
    from services.api_audit_service import ApiAuditService
    from services.api_client_service import ApiClientService

logger = logging.getLogger(__name__)

_api_client_service: Optional["ApiClientService"] = None
_api_audit_service: Optional["ApiAuditService"] = None


def set_auth_services(
    client_svc: "ApiClientService",
    audit_svc: "ApiAuditService",
) -> None:
    """Wire the auth and audit services used by :func:`get_authenticated_client`."""
    global _api_client_service, _api_audit_service
    _api_client_service = client_svc
    _api_audit_service = audit_svc


async def get_authenticated_client(
    request: Request,
    x_client_id: Optional[str] = Header(default=None, alias="X-Client-Id"),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> ApiClient:
    """FastAPI dependency: authenticate and authorize the calling client.

    Raises:
        HTTPException 401: When credentials are missing or invalid.
        HTTPException 403: When the client lacks access to the endpoint.
        HTTPException 503: When the auth service has not been initialised.
    """
    start = time.monotonic()
    request_id = getattr(request.state, "request_id", None)
    remote_ip = request.client.host if request.client else None
    endpoint = request.url.path

    if not x_client_id or not x_api_key:
        _log_audit(
            "unknown",
            endpoint,
            request.method,
            401,
            remote_ip,
            request_id,
            int((time.monotonic() - start) * 1000),
            False,
            "Missing auth headers",
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Client-Id or X-API-Key headers",
        )

    try:
        if _api_client_service is None:
            raise HTTPException(status_code=503, detail="Auth service unavailable")

        client = _api_client_service.authenticate_and_authorize(
            x_client_id, x_api_key, endpoint
        )
        _log_audit(
            x_client_id,
            endpoint,
            request.method,
            200,
            remote_ip,
            request_id,
            int((time.monotonic() - start) * 1000),
            True,
        )
        return client

    except AuthenticationError as e:
        logger.warning("Authentication failed for client '%s': %s", x_client_id, e)
        _log_audit(
            x_client_id,
            endpoint,
            request.method,
            401,
            remote_ip,
            request_id,
            int((time.monotonic() - start) * 1000),
            False,
            str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )

    except AuthorizationError as e:
        logger.warning(
            "Authorization failed for client '%s' on '%s': %s",
            x_client_id,
            endpoint,
            e,
        )
        _log_audit(
            x_client_id,
            endpoint,
            request.method,
            403,
            remote_ip,
            request_id,
            int((time.monotonic() - start) * 1000),
            False,
            str(e),
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Endpoint access denied",
        )


def _log_audit(
    client_id: str,
    endpoint: str,
    method: str,
    status_code: int,
    remote_ip: Optional[str],
    request_id: Optional[str],
    duration_ms: int,
    is_auth: bool,
    error: Optional[str] = None,
) -> None:
    """Write an audit log entry, swallowing any write errors."""
    if _api_audit_service:
        try:
            _api_audit_service.log_request(
                client_id=client_id,
                endpoint_path=endpoint,
                method=method,
                status_code=status_code,
                remote_ip=remote_ip,
                request_id=request_id,
                response_time_ms=duration_ms,
                is_authorized=is_auth,
                error_message=error,
            )
        except Exception as exc:
            logger.debug("Audit log write failed: %s", exc)
