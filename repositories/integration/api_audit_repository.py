"""Repository for ApiAuditLogs table operations."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_API_AUDIT_LOGS

logger = logging.getLogger(__name__)


class ApiAuditRepository(BaseRepository):
    """Provides operations against the ``ApiAuditLogs`` table."""

    def insert_audit_log(
        self,
        client_id: str,
        endpoint_path: str,
        method: str,
        status_code: int,
        remote_ip: Optional[str],
        request_id: Optional[str],
        response_time_ms: Optional[int],
        is_authorized: bool,
        error_message: Optional[str] = None,
    ) -> None:
        """Persist a single API access audit log entry.

        Args:
            client_id: Identifier of the API client that made the request.
            endpoint_path: HTTP path of the requested endpoint.
            method: HTTP method (GET, POST, etc.).
            status_code: HTTP response status code.
            remote_ip: IP address of the caller, if available.
            request_id: Correlation ID for the request, if provided.
            response_time_ms: Time taken to produce the response in milliseconds.
            is_authorized: Whether the request passed authorization checks.
            error_message: Error detail for failed/unauthorized requests.
        """
        sql = f"""
            INSERT INTO {TABLE_API_AUDIT_LOGS}
                (ClientId, EndpointPath, Method, StatusCode, RemoteIp,
                 RequestId, ResponseTimeMs, IsAuthorized, ErrorMessage, CreatedAt)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE())
        """
        auth_flag = 1 if is_authorized else 0
        err_str = error_message[:2000] if error_message else None
        self.execute_non_query(
            sql,
            (
                client_id,
                endpoint_path,
                method,
                status_code,
                remote_ip,
                request_id,
                response_time_ms,
                auth_flag,
                err_str,
            ),
        )

    def get_recent_logs(self, limit: int = 100) -> List[dict]:
        """Return the most recent audit log entries.

        Args:
            limit: Maximum number of rows to return.

        Returns:
            A list of row dicts ordered by CreatedAt descending.
        """
        sql = f"""
            SELECT TOP (?) *
              FROM {TABLE_API_AUDIT_LOGS}
             ORDER BY CreatedAt DESC
        """
        rows = self.execute_query(sql, (limit,))
        return self.rows_to_dicts(rows)
