"""Repository for DMS invoice data access."""
import logging

from repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class DmsInvoiceRepository(BaseRepository):
    """Provides write access to invoice data in the DMS database."""

    def upsert_invoice(self, invoice_data: dict) -> None:
        """Insert or update an invoice record in the DMS database.

        Uses a SQL Server MERGE statement keyed on ``HmisInvoiceId`` so that
        new invoices are inserted and existing ones are updated atomically.

        Expected keys in *invoice_data*:
            - ``HmisInvoiceId`` (str): Primary key from the HMIS source system.
            - ``InvoiceNumber`` (str): Human-readable invoice reference.
            - ``HmisOrderId`` (str, optional): Associated HMIS order identifier.
            - ``CustomerId`` (str): Associated customer identifier.
            - ``InvoiceDate`` (str, optional): ISO-8601 invoice date.
            - ``TotalAmount`` (float, optional): Invoice total amount.
            - ``PaidAmount`` (float, optional): Amount already paid.
            - ``Status`` (str, optional): Current invoice status.

        Args:
            invoice_data: Dictionary of field values for the invoice record.
        """
        sql = """
            MERGE INTO DmsInvoices AS target
            USING (SELECT ? AS HmisInvoiceId) AS source
              ON target.HmisInvoiceId = source.HmisInvoiceId
            WHEN MATCHED THEN
                UPDATE SET
                    InvoiceNumber = ?,
                    HmisOrderId   = ?,
                    CustomerId    = ?,
                    InvoiceDate   = ?,
                    TotalAmount   = ?,
                    PaidAmount    = ?,
                    Status        = ?,
                    ModifiedAt    = GETUTCDATE(),
                    SyncedAt      = GETUTCDATE()
            WHEN NOT MATCHED THEN
                INSERT (HmisInvoiceId, InvoiceNumber, HmisOrderId, CustomerId,
                        InvoiceDate, TotalAmount, PaidAmount, Status,
                        CreatedAt, ModifiedAt, SyncedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE(), GETUTCDATE(), GETUTCDATE());
        """
        hmis_invoice_id = invoice_data.get("HmisInvoiceId")
        invoice_number = invoice_data.get("InvoiceNumber")
        hmis_order_id = invoice_data.get("HmisOrderId")
        customer_id = invoice_data.get("CustomerId")
        invoice_date = invoice_data.get("InvoiceDate")
        total_amount = invoice_data.get("TotalAmount")
        paid_amount = invoice_data.get("PaidAmount")
        status = invoice_data.get("Status")

        self.execute_non_query(
            sql,
            (
                # USING source
                hmis_invoice_id,
                # UPDATE SET
                invoice_number, hmis_order_id, customer_id, invoice_date,
                total_amount, paid_amount, status,
                # INSERT VALUES
                hmis_invoice_id, invoice_number, hmis_order_id, customer_id,
                invoice_date, total_amount, paid_amount, status,
            ),
        )
        logger.debug("Upserted DMS invoice '%s'.", hmis_invoice_id)
