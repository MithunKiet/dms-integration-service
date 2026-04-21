"""Service for API client authentication and authorization."""
from typing import Optional
import json
import logging

from repositories.integration.api_client_repository import ApiClientRepository
from models.api_client import ApiClient
from core.security import verify_api_key, is_endpoint_allowed
from core.exceptions import AuthenticationError, AuthorizationError
from core.utils import utc_now

logger = logging.getLogger(__name__)


class ApiClientService:
    """Authenticates and authorises API clients for incoming requests."""

    def __init__(self, client_repo: ApiClientRepository) -> None:
        self._repo = client_repo

    def authenticate_and_authorize(
        self,
        client_id: str,
        raw_api_key: str,
        endpoint_path: str,
    ) -> ApiClient:
        """Validate an API client's credentials and endpoint permission.

        Steps performed:
        1. Look up the client record; raise if not found.
        2. Verify the client is active.
        3. Verify the raw API key against the stored bcrypt hash.
        4. Check the key has not expired.
        5. Verify the requested endpoint is in the client's allowlist.
        6. Update the client's ``LastUsedAt`` timestamp (best-effort).

        Args:
            client_id: The unique identifier submitted by the caller.
            raw_api_key: The plain-text API key submitted by the caller.
            endpoint_path: The HTTP path being requested.

        Returns:
            A populated :class:`~models.api_client.ApiClient` on success.

        Raises:
            :class:`~core.exceptions.AuthenticationError`: On bad credentials or
                inactive/expired client.
            :class:`~core.exceptions.AuthorizationError`: If the endpoint is not
                in the client's allowlist.
        """
        row = self._repo.get_client(client_id)
        if not row:
            raise AuthenticationError(f"Client not found: {client_id}")

        if not row.get("IsActive", False):
            raise AuthenticationError(f"Client is inactive: {client_id}")

        key_hash = row.get("KeyHash", "")
        if not verify_api_key(raw_api_key, key_hash):
            raise AuthenticationError("Invalid API key")

        expires_at = row.get("ExpiresAt")
        if expires_at and utc_now() > expires_at:
            raise AuthenticationError("API key has expired")

        allowed_str = row.get("AllowedEndpoints", "")
        allowed_list = json.loads(allowed_str) if allowed_str else []

        if not is_endpoint_allowed(allowed_list, endpoint_path):
            raise AuthorizationError(
                f"Endpoint not allowed for client '{client_id}': {endpoint_path}"
            )

        try:
            self._repo.update_last_used(client_id)
        except Exception as e:
            logger.warning("Failed to update last_used for client '%s': %s", client_id, e)

        return ApiClient(
            client_id=client_id,
            client_name=row.get("ClientName", ""),
            key_prefix=row.get("KeyPrefix"),
            key_hash=key_hash,
            is_active=True,
            allowed_endpoints=allowed_list,
            rate_limit_per_minute=row.get("RateLimitPerMinute", 60),
            created_at=row.get("CreatedAt"),
            expires_at=expires_at,
            last_used_at=row.get("LastUsedAt"),
        )
