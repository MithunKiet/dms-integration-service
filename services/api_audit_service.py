"""Service for writing API access audit logs."""
from typing import Optional
import logging

from repositories.integration.api_audit_repository import ApiAuditRepository

logger = logging.getLogger(__name__)


class ApiAuditService:
    """Writes API request audit entries to the ApiAuditLogs table."""

    def __init__(self, audit_repo: ApiAuditRepository) -> None:
        self._repo = audit_repo

    def log_request(
        self,
        client_id: str,
        endpoint_path: str,
        method: str,
        status_code: int,
        remote_ip: Optional[str] = None,
        request_id: Optional[str] = None,
        response_time_ms: Optional[int] = None,
        is_authorized: bool = True,
        error_message: Optional[str] = None,
    ) -> None:
        """Persist an API request audit log entry.

        Failures are caught and logged rather than propagated, so that an audit
        write failure never causes an API response failure.

        Args:
            client_id: Identifier of the API client that made the request.
            endpoint_path: HTTP path of the requested endpoint.
            method: HTTP method (GET, POST, etc.).
            status_code: HTTP response status code returned to the caller.
            remote_ip: IP address of the caller, if available.
            request_id: Correlation ID for the request, if provided.
            response_time_ms: Time taken to produce the response in milliseconds.
            is_authorized: Whether the request passed authorization checks.
            error_message: Error detail for failed or unauthorized requests.
        """
        try:
            self._repo.insert_audit_log(
                client_id=client_id,
                endpoint_path=endpoint_path,
                method=method,
                status_code=status_code,
                remote_ip=remote_ip,
                request_id=request_id,
                response_time_ms=response_time_ms,
                is_authorized=is_authorized,
                error_message=error_message,
            )
        except Exception as e:
            logger.error("Failed to write API audit log: %s", e)
