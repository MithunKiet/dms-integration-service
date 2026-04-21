"""Repository for HMIS customer data access."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)

_CUSTOMER_COLUMNS = (
    "CustomerId, CustomerName, CustomerCode, ContactPhone, "
    "ContactEmail, Address, City, State, IsActive, ModifiedAt"
)


class HmisCustomerRepository(BaseRepository):
    """Provides read access to customer data in the HMIS database."""

    def get_customers_since(
        self,
        last_sync_value: Optional[str],
        batch_size: int = 500,
    ) -> List[dict]:
        """Return a batch of customers modified after *last_sync_value*.

        When *last_sync_value* is ``None`` (first run), all customers are
        returned up to *batch_size*.  Otherwise only rows with
        ``ModifiedAt > last_sync_value`` are returned, ordered by
        ``ModifiedAt ASC`` for stable watermark-based pagination.

        Args:
            last_sync_value: ISO-8601 timestamp string from the last checkpoint,
                or ``None`` to perform a full initial load.
            batch_size: Maximum number of rows to return.

        Returns:
            A list of dicts containing customer fields.
        """
        if last_sync_value:
            sql = f"""
                SELECT TOP (?) {_CUSTOMER_COLUMNS}
                  FROM Customers
                 WHERE ModifiedAt > ?
                 ORDER BY ModifiedAt ASC
            """
            rows = self.execute_query(sql, (batch_size, last_sync_value))
        else:
            sql = f"""
                SELECT TOP (?) {_CUSTOMER_COLUMNS}
                  FROM Customers
                 ORDER BY ModifiedAt ASC
            """
            rows = self.execute_query(sql, (batch_size,))

        return self.rows_to_dicts(rows)
