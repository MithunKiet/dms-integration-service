"""Repository for API client configuration loaded from settings."""
import json
from typing import List, Optional

from config.settings import Settings
from repositories.base_repository import BaseRepository


class ApiClientRepository(BaseRepository):
    """Provides API client records sourced from ``API_KEY_SETTINGS``."""

    def get_client(self, client_id: str) -> Optional[dict]:
        """Fetch a single API client record by its unique ID.

        Args:
            client_id: The unique identifier for the API client.

        Returns:
            A row dict, or ``None`` if the client does not exist.
        """
        env_client = self._get_client_from_env(client_id)
        if env_client is not None:
            return env_client

        return None

    @staticmethod
    def _get_client_from_env(client_id: str) -> Optional[dict]:
        """Resolve a client from ``API_KEY_SETTINGS`` and map it to row-style keys."""
        settings = Settings.get_settings()
        for client in settings.api_key_settings.clients:
            if client.client_id.lower() != client_id.lower():
                continue

            key_hash = client.api_key if client.api_key.startswith("$2") else ""
            raw_api_key = "" if key_hash else client.api_key
            return {
                "ClientId": client.client_id,
                "ClientName": client.client_name,
                "KeyPrefix": None,
                "KeyHash": key_hash,
                "RawApiKey": raw_api_key,
                "AllowedEndpoints": json.dumps(client.allowed_endpoints),
                "RateLimitPerMinute": 60,
                "IsActive": client.is_active,
                "CreatedAt": None,
                "ExpiresAt": None,
                "LastUsedAt": None,
            }
        return None

    def update_last_used(self, client_id: str) -> None:
        """Best-effort hook kept for compatibility.

        Args:
            client_id: The unique identifier for the API client.
        """
        _ = client_id

    def list_clients(self) -> List[dict]:
        """Return all registered API clients from settings.

        Returns:
            A list of row dicts ordered by ClientId.
        """
        settings = Settings.get_settings()
        rows: List[dict] = []
        for client in settings.api_key_settings.clients:
            rows.append(
                {
                    "ClientId": client.client_id,
                    "ClientName": client.client_name,
                    "KeyPrefix": None,
                    "KeyHash": client.api_key if client.api_key.startswith("$2") else "",
                    "RawApiKey": "" if client.api_key.startswith("$2") else client.api_key,
                    "AllowedEndpoints": json.dumps(client.allowed_endpoints),
                    "RateLimitPerMinute": 60,
                    "IsActive": client.is_active,
                    "CreatedAt": None,
                    "ExpiresAt": None,
                    "LastUsedAt": None,
                }
            )
        return sorted(rows, key=lambda item: str(item.get("ClientId", "")))

    def insert_client(
        self,
        client_id: str,
        client_name: str,
        key_prefix: str,
        key_hash: str,
        allowed_endpoints: str,
        rate_limit: int = 60,
    ) -> None:
        """Insert is not supported for env-backed clients.

        Args:
            client_id: Unique identifier for the client (e.g. ``"svc-erp"``).
            client_name: Human-readable display name.
            key_prefix: Short prefix used in the raw API key.
            key_hash: bcrypt hash of the full raw API key.
            allowed_endpoints: JSON array string of permitted endpoint paths.
            rate_limit: Maximum requests per minute allowed for this client.
        """
        raise NotImplementedError(
            "insert_client is not supported when API clients are sourced from API_KEY_SETTINGS"
        )
