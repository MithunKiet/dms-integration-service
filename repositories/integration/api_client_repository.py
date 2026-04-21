"""Repository for ApiClients table operations."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_API_CLIENTS

logger = logging.getLogger(__name__)


class ApiClientRepository(BaseRepository):
    """Provides operations against the ``ApiClients`` table."""

    def get_client(self, client_id: str) -> Optional[dict]:
        """Fetch a single API client record by its unique ID.

        Args:
            client_id: The unique identifier for the API client.

        Returns:
            A row dict, or ``None`` if the client does not exist.
        """
        sql = f"SELECT * FROM {TABLE_API_CLIENTS} WHERE ClientId = ?"
        rows = self.execute_query(sql, (client_id,))
        if rows:
            return self.row_to_dict(rows[0])
        return None

    def update_last_used(self, client_id: str) -> None:
        """Update the LastUsedAt timestamp for a client after a successful request.

        Args:
            client_id: The unique identifier for the API client.
        """
        sql = f"""
            UPDATE {TABLE_API_CLIENTS}
               SET LastUsedAt = GETUTCDATE()
             WHERE ClientId   = ?
        """
        self.execute_non_query(sql, (client_id,))

    def list_clients(self) -> List[dict]:
        """Return all registered API client records.

        Returns:
            A list of row dicts ordered by ClientId.
        """
        sql = f"SELECT * FROM {TABLE_API_CLIENTS} ORDER BY ClientId"
        rows = self.execute_query(sql)
        return self.rows_to_dicts(rows)

    def insert_client(
        self,
        client_id: str,
        client_name: str,
        key_prefix: str,
        key_hash: str,
        allowed_endpoints: str,
        rate_limit: int = 60,
    ) -> None:
        """Insert a new API client record.

        Args:
            client_id: Unique identifier for the client (e.g. ``"svc-erp"``).
            client_name: Human-readable display name.
            key_prefix: Short prefix used in the raw API key.
            key_hash: bcrypt hash of the full raw API key.
            allowed_endpoints: JSON array string of permitted endpoint paths.
            rate_limit: Maximum requests per minute allowed for this client.
        """
        sql = f"""
            INSERT INTO {TABLE_API_CLIENTS}
                (ClientId, ClientName, KeyPrefix, KeyHash, AllowedEndpoints,
                 RateLimitPerMinute, IsActive, CreatedAt)
            VALUES (?, ?, ?, ?, ?, ?, 1, GETUTCDATE())
        """
        self.execute_non_query(
            sql,
            (client_id, client_name, key_prefix, key_hash, allowed_endpoints, rate_limit),
        )
        logger.info("Inserted API client '%s'.", client_id)
