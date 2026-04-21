"""Request logging and correlation middleware."""
from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from core.utils import generate_request_id

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log every HTTP request with timing, status, and a correlation ID.

    Adds an ``X-Request-Id`` header to every response and never logs the
    value of sensitive headers such as ``X-API-Key``.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        """Wrap the request/response cycle with logging and timing."""
        request_id = generate_request_id()
        start_time = time.monotonic()

        # Make the correlation ID available to route handlers.
        request.state.request_id = request_id

        response = await call_next(request)

        duration_ms = int((time.monotonic() - start_time) * 1000)
        response.headers["X-Request-Id"] = request_id

        client_ip = request.client.host if request.client else "unknown"
        logger.info(
            "API %s %s -> %d (%dms) ip=%s rid=%s",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            client_ip,
            request_id,
        )
        return response
