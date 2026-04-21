"""HMIS to DMS customer synchronization job."""
import json
import logging
from typing import Optional

from config.constants import DEFAULT_BATCH_SIZE
from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_hmis_customer_repo = None
_dms_customer_repo = None
_failed_record_repo = None


def set_dependencies(hmis_customer_repo, dms_customer_repo, failed_record_repo) -> None:
    """Inject repositories at startup."""
    global _hmis_customer_repo, _dms_customer_repo, _failed_record_repo
    _hmis_customer_repo = hmis_customer_repo
    _dms_customer_repo = dms_customer_repo
    _failed_record_repo = failed_record_repo


def run_hmis_to_dms_customer_sync(context: JobContext) -> JobResult:
    """Sync customers from HMIS to DMS using incremental watermark strategy."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if not _hmis_customer_repo or not _dms_customer_repo:
        logger.warning("customer_sync_job: repositories not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    last_sync = context.checkpoint.last_sync_value if context.checkpoint else None
    logger.info("Customer sync starting. Last checkpoint: %s", last_sync)

    customers = _hmis_customer_repo.get_customers_since(
        last_sync, batch_size=DEFAULT_BATCH_SIZE
    )
    result.records_read = len(customers)

    if not customers:
        logger.info("No new customers to sync.")
        return result

    processed = 0
    failed = 0
    latest_modified_at: Optional[str] = None

    for customer in customers:
        try:
            mapped = _map_customer(customer)
            if not mapped.get("CustomerId"):
                raise ValueError("Missing CustomerId in mapped customer")
            _dms_customer_repo.upsert_customer(mapped)
            processed += 1
            mod_at = customer.get("ModifiedAt")
            if mod_at:
                latest_modified_at = str(mod_at)
        except Exception as e:
            failed += 1
            logger.error("Failed to sync customer %s: %s", customer.get("CustomerId"), e)
            if _failed_record_repo:
                try:
                    _failed_record_repo.save_failed_record(
                        job_name=context.job_name,
                        source_id=str(customer.get("CustomerId", "unknown")),
                        source_table="HmisCustomers",
                        error_message=str(e)[:2000],
                        raw_data=json.dumps(customer, default=str)[:4000],
                    )
                except Exception as save_err:
                    logger.error("Failed to save failed record: %s", save_err)

    result.records_processed = processed
    result.records_failed = failed
    if latest_modified_at:
        result.checkpoint_value = latest_modified_at
    if failed > 0 and processed == 0:
        result.status = JobStatus.FAILED

    logger.info("Customer sync completed: processed=%d failed=%d", processed, failed)
    return result


def _map_customer(hmis_record: dict) -> dict:
    """Map HMIS customer fields to DMS customer fields."""
    return {
        "CustomerId": hmis_record.get("CustomerId"),
        "CustomerName": hmis_record.get("CustomerName"),
        "CustomerCode": hmis_record.get("CustomerCode"),
        "ContactPhone": hmis_record.get("ContactPhone"),
        "ContactEmail": hmis_record.get("ContactEmail"),
        "Address": hmis_record.get("Address"),
        "City": hmis_record.get("City"),
        "State": hmis_record.get("State"),
        "IsActive": hmis_record.get("IsActive", True),
        "HmisModifiedAt": hmis_record.get("ModifiedAt"),
    }
