"""Repository for DMS product data access."""
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class DmsProductRepository(BaseRepository):
    """Provides write access to product data in the DMS database."""

    def upsert_product(self, product_data: dict) -> None:
        """Insert or update a product record in the DMS database.

        Uses a SQL Server MERGE statement keyed on ``ProductId`` so that
        new products are inserted and existing ones are updated atomically.

        Expected keys in *product_data*:
            - ``ProductId`` (str): Primary key from the source system.
            - ``ProductName`` (str): Full product name.
            - ``SKU`` (str, optional): Stock-keeping unit code.
            - ``Category`` (str, optional): Product category.
            - ``Price`` (float, optional): Unit price.
            - ``IsActive`` (bool/int): Whether the product is active.

        Args:
            product_data: Dictionary of field values for the product record.
        """
        sql = """
            MERGE INTO DmsProducts AS target
            USING (SELECT ? AS ProductId) AS source
              ON target.ProductId = source.ProductId
            WHEN MATCHED THEN
                UPDATE SET
                    ProductName  = ?,
                    SKU          = ?,
                    Category     = ?,
                    Price        = ?,
                    IsActive     = ?,
                    ModifiedAt   = GETUTCDATE(),
                    SyncedAt     = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (ProductId, ProductName, SKU, Category, Price, IsActive,
                        CreatedAt, ModifiedAt, SyncedAt)
                VALUES (?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE(), GETUTCDATE());
        """
        product_id = product_data.get("ProductId")
        product_name = product_data.get("ProductName")
        sku = product_data.get("SKU")
        category = product_data.get("Category")
        price = product_data.get("Price")
        is_active = 1 if product_data.get("IsActive") else 0

        self.execute_non_query(
            sql,
            (
                # USING source
                product_id,
                # UPDATE SET
                product_name, sku, category, price, is_active,
                # INSERT VALUES
                product_id, product_name, sku, category, price, is_active,
            ),
        )
        logger.debug("Upserted DMS product '%s'.", product_id)
