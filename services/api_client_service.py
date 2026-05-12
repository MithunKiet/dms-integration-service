"""Service for API client authentication and authorization."""
from __future__ import annotations

from datetime import datetime
from hmac import compare_digest
from typing import Dict, Optional
import json
import logging

from config.settings import ApiKeyClientConfig
from core.exceptions import AuthenticationError, AuthorizationError
from core.security import is_endpoint_allowed, verify_api_key
from core.utils import utc_now
from models.api_client import ApiClient
from repositories.integration.api_client_repository import ApiClientRepository

logger = logging.getLogger(__name__)


class ApiClientService:
    """Authenticates and authorises API clients for incoming requests."""

    def __init__(
        self,
        client_repo: Optional[ApiClientRepository] = None,
        configured_clients: Optional[list[ApiKeyClientConfig]] = None,
    ) -> None:
        self._repo = client_repo
        self._configured_clients: Dict[str, ApiKeyClientConfig] = {
            client.client_id.lower(): client for client in (configured_clients or [])
        }

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
        configured_client = self._configured_clients.get(client_id.lower())
        if configured_client is not None:
            return self._authenticate_from_config(
                configured_client=configured_client,
                raw_api_key=raw_api_key,
                endpoint_path=endpoint_path,
            )

        if self._repo is None:
            raise AuthenticationError("Auth service is not configured")

        row = self._repo.get_client(client_id)
        if not row:
            raise AuthenticationError(f"Client not found: {client_id}")

        if not row.get("IsActive", False):
            raise AuthenticationError(f"Client is inactive: {client_id}")

        key_hash = row.get("KeyHash", "")
        raw_key_from_store = row.get("RawApiKey", "")
        key_matches = compare_digest(raw_api_key, raw_key_from_store) if raw_key_from_store else False
        if not key_matches and key_hash:
            key_matches = verify_api_key(raw_api_key, key_hash)
        if not key_matches:
            raise AuthenticationError("Invalid API key")

        expires_at = row.get("ExpiresAt")
        if isinstance(expires_at, datetime) and utc_now() > expires_at:
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
            expires_at=expires_at if isinstance(expires_at, datetime) else None,
            last_used_at=row.get("LastUsedAt"),
        )

    def _authenticate_from_config(
        self,
        configured_client: ApiKeyClientConfig,
        raw_api_key: str,
        endpoint_path: str,
    ) -> ApiClient:
        """Authenticate against client entries supplied through app settings."""
        if not configured_client.is_active:
            raise AuthenticationError(f"Client is inactive: {configured_client.client_id}")

        # Allow either plain-text API keys from env or pre-hashed bcrypt values.
        key_matches = compare_digest(raw_api_key, configured_client.api_key)
        if not key_matches and configured_client.api_key.startswith("$2"):
            key_matches = verify_api_key(raw_api_key, configured_client.api_key)

        if not key_matches:
            raise AuthenticationError("Invalid API key")

        allowed_list = [
            item if item.startswith("/") or item == "*" else f"/{item}"
            for item in configured_client.allowed_endpoints
        ]
        normalized_endpoint = (
            endpoint_path if endpoint_path.startswith("/") else f"/{endpoint_path}"
        )

        if not is_endpoint_allowed(allowed_list, normalized_endpoint):
            raise AuthorizationError(
                f"Endpoint not allowed for client '{configured_client.client_id}': {endpoint_path}"
            )

        return ApiClient(
            client_id=configured_client.client_id,
            client_name=configured_client.client_name,
            key_prefix=None,
            key_hash=configured_client.api_key if configured_client.api_key.startswith("$2") else "",
            is_active=True,
            allowed_endpoints=allowed_list,
            created_at=utc_now(),
            last_used_at=utc_now(),
        )

