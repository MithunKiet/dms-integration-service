"""Repository for DMS stock data access."""
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class DmsStockRepository(BaseRepository):
    """Provides write access to stock data in the DMS database."""

    def upsert_stock(self, stock_data: dict) -> None:
        """Insert or update a stock record in the DMS database.

        Uses a SQL Server MERGE statement keyed on ``StockId`` so that
        new stock records are inserted and existing ones are updated atomically.

        Expected keys in *stock_data*:
            - ``StockId`` (str): Primary key from the source system.
            - ``ProductId`` (str): Associated product identifier.
            - ``DealerId`` (str, optional): Associated dealer identifier.
            - ``Quantity`` (int/float, optional): Quantity on hand.
            - ``ReorderLevel`` (int/float, optional): Reorder threshold.
            - ``Warehouse`` (str, optional): Warehouse location code.

        Args:
            stock_data: Dictionary of field values for the stock record.
        """
        sql = """
            MERGE INTO DmsStock AS target
            USING (SELECT ? AS StockId) AS source
              ON target.StockId = source.StockId
            WHEN MATCHED THEN
                UPDATE SET
                    ProductId    = ?,
                    DealerId     = ?,
                    Quantity     = ?,
                    ReorderLevel = ?,
                    Warehouse    = ?,
                    ModifiedAt   = GETUTCDATE(),
                    SyncedAt     = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (StockId, ProductId, DealerId, Quantity, ReorderLevel,
                        Warehouse, CreatedAt, ModifiedAt, SyncedAt)
                VALUES (?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE(), GETUTCDATE());
        """
        stock_id = stock_data.get("StockId")
        product_id = stock_data.get("ProductId")
        dealer_id = stock_data.get("DealerId")
        quantity = stock_data.get("Quantity")
        reorder_level = stock_data.get("ReorderLevel")
        warehouse = stock_data.get("Warehouse")

        self.execute_non_query(
            sql,
            (
                # USING source
                stock_id,
                # UPDATE SET
                product_id, dealer_id, quantity, reorder_level, warehouse,
                # INSERT VALUES
                stock_id, product_id, dealer_id, quantity, reorder_level, warehouse,
            ),
        )
        logger.debug("Upserted DMS stock record '%s'.", stock_id)
