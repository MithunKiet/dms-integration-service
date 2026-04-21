"""Repository for DMS dealer data access."""
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class DmsDealerRepository(BaseRepository):
    """Provides write access to dealer data in the DMS database."""

    def upsert_dealer(self, dealer_data: dict) -> None:
        """Insert or update a dealer record in the DMS database.

        Uses a SQL Server MERGE statement keyed on ``DealerId`` so that
        new dealers are inserted and existing ones are updated atomically.

        Expected keys in *dealer_data*:
            - ``DealerId`` (str): Primary key from the source system.
            - ``DealerName`` (str): Full dealer name.
            - ``DealerCode`` (str, optional): Short code identifier.
            - ``Region`` (str, optional): Region or territory code.
            - ``ContactPhone`` (str, optional): Phone number.
            - ``ContactEmail`` (str, optional): Email address.
            - ``Address`` (str, optional): Street address.
            - ``IsActive`` (bool/int): Whether the dealer is active.

        Args:
            dealer_data: Dictionary of field values for the dealer record.
        """
        sql = """
            MERGE INTO DmsDealers AS target
            USING (SELECT ? AS DealerId) AS source
              ON target.DealerId = source.DealerId
            WHEN MATCHED THEN
                UPDATE SET
                    DealerName   = ?,
                    DealerCode   = ?,
                    Region       = ?,
                    ContactPhone = ?,
                    ContactEmail = ?,
                    Address      = ?,
                    IsActive     = ?,
                    ModifiedAt   = GETUTCDATE(),
                    SyncedAt     = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (DealerId, DealerName, DealerCode, Region,
                        ContactPhone, ContactEmail, Address, IsActive,
                        CreatedAt, ModifiedAt, SyncedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE(), GETUTCDATE());
        """
        dealer_id = dealer_data.get("DealerId")
        dealer_name = dealer_data.get("DealerName")
        dealer_code = dealer_data.get("DealerCode")
        region = dealer_data.get("Region")
        contact_phone = dealer_data.get("ContactPhone")
        contact_email = dealer_data.get("ContactEmail")
        address = dealer_data.get("Address")
        is_active = 1 if dealer_data.get("IsActive") else 0

        self.execute_non_query(
            sql,
            (
                # USING source
                dealer_id,
                # UPDATE SET
                dealer_name, dealer_code, region, contact_phone, contact_email,
                address, is_active,
                # INSERT VALUES
                dealer_id, dealer_name, dealer_code, region, contact_phone,
                contact_email, address, is_active,
            ),
        )
        logger.debug("Upserted DMS dealer '%s'.", dealer_id)
