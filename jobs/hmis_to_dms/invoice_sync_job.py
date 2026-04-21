"""HMIS to DMS invoice synchronization job."""
import json
import logging
from typing import Optional

from config.constants import DEFAULT_BATCH_SIZE
from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_hmis_invoice_repo = None
_dms_invoice_repo = None
_failed_record_repo = None


def set_dependencies(hmis_invoice_repo, dms_invoice_repo, failed_record_repo) -> None:
    """Inject repositories at startup."""
    global _hmis_invoice_repo, _dms_invoice_repo, _failed_record_repo
    _hmis_invoice_repo = hmis_invoice_repo
    _dms_invoice_repo = dms_invoice_repo
    _failed_record_repo = failed_record_repo


def run_hmis_to_dms_invoice_sync(context: JobContext) -> JobResult:
    """Sync invoices from HMIS to DMS using incremental watermark strategy."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if not _hmis_invoice_repo or not _dms_invoice_repo:
        logger.warning("invoice_sync_job: repositories not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    last_sync = context.checkpoint.last_sync_value if context.checkpoint else None
    logger.info("Invoice sync (HMIS->DMS) starting. Last checkpoint: %s", last_sync)

    invoices = _hmis_invoice_repo.get_invoices_since(
        last_sync, batch_size=DEFAULT_BATCH_SIZE
    )
    result.records_read = len(invoices)

    if not invoices:
        logger.info("No new invoices to sync.")
        return result

    processed = 0
    failed = 0
    latest_modified_at: Optional[str] = None

    for invoice in invoices:
        try:
            mapped = _map_invoice(invoice)
            if not mapped.get("HmisInvoiceId"):
                raise ValueError("Missing HmisInvoiceId in mapped invoice")
            _dms_invoice_repo.upsert_invoice(mapped)
            processed += 1
            mod_at = invoice.get("ModifiedAt")
            if mod_at:
                latest_modified_at = str(mod_at)
        except Exception as e:
            failed += 1
            logger.error("Failed to sync invoice %s: %s", invoice.get("InvoiceId"), e)
            if _failed_record_repo:
                try:
                    _failed_record_repo.save_failed_record(
                        job_name=context.job_name,
                        source_id=str(invoice.get("InvoiceId", "unknown")),
                        source_table="HmisInvoices",
                        error_message=str(e)[:2000],
                        raw_data=json.dumps(invoice, default=str)[:4000],
                    )
                except Exception as save_err:
                    logger.error("Failed to save failed record: %s", save_err)

    result.records_processed = processed
    result.records_failed = failed
    if latest_modified_at:
        result.checkpoint_value = latest_modified_at
    if failed > 0 and processed == 0:
        result.status = JobStatus.FAILED

    logger.info("Invoice sync completed: processed=%d failed=%d", processed, failed)
    return result


def _map_invoice(hmis_record: dict) -> dict:
    """Map HMIS invoice fields to DMS invoice fields."""
    return {
        "HmisInvoiceId": hmis_record.get("InvoiceId"),
        "InvoiceNumber": hmis_record.get("InvoiceNumber"),
        "HmisOrderId": hmis_record.get("OrderId"),
        "CustomerId": hmis_record.get("CustomerId"),
        "InvoiceDate": hmis_record.get("InvoiceDate"),
        "TotalAmount": hmis_record.get("TotalAmount"),
        "PaidAmount": hmis_record.get("PaidAmount"),
        "Status": hmis_record.get("Status"),
        "HmisModifiedAt": hmis_record.get("ModifiedAt"),
    }
