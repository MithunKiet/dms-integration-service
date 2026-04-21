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

    def upsert_order(self, order_data: dict) -> None:
        """Insert or update an order record in the DMS database.

        Uses a SQL Server MERGE statement keyed on ``HmisOrderId`` so that
        new orders are inserted and existing ones are updated atomically.

        Expected keys in *order_data*:
            - ``HmisOrderId`` (str): Primary key from the HMIS source system.
            - ``OrderNumber`` (str): Human-readable order reference.
            - ``CustomerId`` (str): Associated customer identifier.
            - ``DealerId`` (str, optional): Associated dealer identifier.
            - ``OrderDate`` (str, optional): ISO-8601 order date.
            - ``Amount`` (float, optional): Order total amount.
            - ``Status`` (str, optional): Current order status.

        Args:
            order_data: Dictionary of field values for the order record.
        """
        sql = """
            MERGE INTO DmsOrders AS target
            USING (SELECT ? AS HmisOrderId) AS source
              ON target.HmisOrderId = source.HmisOrderId
            WHEN MATCHED THEN
                UPDATE SET
                    OrderNumber  = ?,
                    CustomerId   = ?,
                    DealerId     = ?,
                    OrderDate    = ?,
                    Amount       = ?,
                    Status       = ?,
                    ModifiedAt   = GETUTCDATE(),
                    SyncedAt     = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (HmisOrderId, OrderNumber, CustomerId, DealerId,
                        OrderDate, Amount, Status,
                        CreatedAt, ModifiedAt, SyncedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE(), GETUTCDATE());
        """
        hmis_order_id = order_data.get("HmisOrderId")
        order_number = order_data.get("OrderNumber")
        customer_id = order_data.get("CustomerId")
        dealer_id = order_data.get("DealerId")
        order_date = order_data.get("OrderDate")
        amount = order_data.get("Amount")
        status = order_data.get("Status")

        self.execute_non_query(
            sql,
            (
                # USING source
                hmis_order_id,
                # UPDATE SET
                order_number, customer_id, dealer_id, order_date, amount, status,
                # INSERT VALUES
                hmis_order_id, order_number, customer_id, dealer_id,
                order_date, amount, status,
            ),
        )
        logger.debug("Upserted DMS order '%s'.", hmis_order_id)
