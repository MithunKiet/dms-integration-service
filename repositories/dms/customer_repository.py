"""Repository for DMS customer data access."""
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class DmsCustomerRepository(BaseRepository):
    """Provides write access to customer data in the DMS database."""

    def upsert_customer(self, customer_data: dict) -> None:
        """Insert or update a customer record in the DMS database.

        Uses a SQL Server MERGE statement keyed on ``CustomerId`` so that
        new customers are inserted and existing ones are updated atomically.

        Expected keys in *customer_data*:
            - ``CustomerId`` (str): Primary key from the source system.
            - ``CustomerName`` (str): Full display name.
            - ``CustomerCode`` (str, optional): Short code identifier.
            - ``ContactPhone`` (str, optional): Phone number.
            - ``ContactEmail`` (str, optional): Email address.
            - ``Address`` (str, optional): Street address.
            - ``City`` (str, optional): City.
            - ``State`` (str, optional): State or province.
            - ``IsActive`` (bool/int): Whether the customer is active.

        Args:
            customer_data: Dictionary of field values for the customer record.
        """
        sql = """
            MERGE INTO DmsCustomers AS target
            USING (SELECT ? AS CustomerId) AS source
              ON target.CustomerId = source.CustomerId
            WHEN MATCHED THEN
                UPDATE SET
                    CustomerName  = ?,
                    CustomerCode  = ?,
                    ContactPhone  = ?,
                    ContactEmail  = ?,
                    Address       = ?,
                    City          = ?,
                    State         = ?,
                    IsActive      = ?,
                    ModifiedAt    = GETUTCDATE(),
                    SyncedAt      = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (CustomerId, CustomerName, CustomerCode, ContactPhone,
                        ContactEmail, Address, City, State, IsActive,
                        CreatedAt, ModifiedAt, SyncedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE(), GETUTCDATE());
        """
        customer_id = customer_data.get("CustomerId")
        customer_name = customer_data.get("CustomerName")
        customer_code = customer_data.get("CustomerCode")
        contact_phone = customer_data.get("ContactPhone")
        contact_email = customer_data.get("ContactEmail")
        address = customer_data.get("Address")
        city = customer_data.get("City")
        state = customer_data.get("State")
        is_active = 1 if customer_data.get("IsActive") else 0

        self.execute_non_query(
            sql,
            (
                # USING source
                customer_id,
                # UPDATE SET
                customer_name, customer_code, contact_phone, contact_email,
                address, city, state, is_active,
                # INSERT VALUES
                customer_id, customer_name, customer_code, contact_phone,
                contact_email, address, city, state, is_active,
            ),
        )
        logger.debug("Upserted DMS customer '%s'.", customer_id)
