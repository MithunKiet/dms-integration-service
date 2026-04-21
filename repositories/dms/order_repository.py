"""Repository for DMS order data access."""
from typing import List, Optional
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)

_ORDER_COLUMNS = (
    "DmsOrderId, DmsOrderNumber, HmisOrderId, CustomerId, "
    "TotalAmount, Status, SyncedAt, ModifiedAt"
)


class DmsOrderRepository(BaseRepository):
    """Provides read access to order data in the DMS database."""

    def get_orders_since(
        self,
        last_sync_value: Optional[str],
        batch_size: int = 500,
    ) -> List[dict]:
        """Return a batch of DMS orders modified after *last_sync_value*.

        When *last_sync_value* is ``None`` (first run), all orders are returned
        up to *batch_size*.  Otherwise only rows with
        ``ModifiedAt > last_sync_value`` are returned, ordered by
        ``ModifiedAt ASC`` for stable watermark-based pagination.

        Args:
            last_sync_value: ISO-8601 timestamp string from the last checkpoint,
                or ``None`` to perform a full initial load.
            batch_size: Maximum number of rows to return.

        Returns:
            A list of dicts containing DMS order fields.
        """
        if last_sync_value:
            sql = f"""
                SELECT TOP (?) {_ORDER_COLUMNS}
                  FROM DmsOrders
                 WHERE ModifiedAt > ?
                 ORDER BY ModifiedAt ASC
            """
            rows = self.execute_query(sql, (batch_size, last_sync_value))
        else:
            sql = f"""
                SELECT TOP (?) {_ORDER_COLUMNS}
                  FROM DmsOrders
                 ORDER BY ModifiedAt ASC
            """
            rows = self.execute_query(sql, (batch_size,))

        return self.rows_to_dicts(rows)
