"""HMIS to DMS order synchronization job."""
import json
import logging
from typing import Optional

from config.constants import DEFAULT_BATCH_SIZE
from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_hmis_order_repo = None
_dms_order_repo = None
_failed_record_repo = None


def set_dependencies(hmis_order_repo, dms_order_repo, failed_record_repo) -> None:
    """Inject repositories at startup."""
    global _hmis_order_repo, _dms_order_repo, _failed_record_repo
    _hmis_order_repo = hmis_order_repo
    _dms_order_repo = dms_order_repo
    _failed_record_repo = failed_record_repo


def run_hmis_to_dms_order_sync(context: JobContext) -> JobResult:
    """Sync orders from HMIS to DMS using incremental watermark strategy."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if not _hmis_order_repo or not _dms_order_repo:
        logger.warning("order_sync_job: repositories not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    last_sync = context.checkpoint.last_sync_value if context.checkpoint else None
    logger.info("Order sync (HMIS->DMS) starting. Last checkpoint: %s", last_sync)

    orders = _hmis_order_repo.get_orders_since(last_sync, batch_size=DEFAULT_BATCH_SIZE)
    result.records_read = len(orders)

    if not orders:
        logger.info("No new orders to sync.")
        return result

    processed = 0
    failed = 0
    latest_modified_at: Optional[str] = None

    for order in orders:
        try:
            mapped = _map_order(order)
            if not mapped.get("HmisOrderId"):
                raise ValueError("Missing HmisOrderId in mapped order")
            _dms_order_repo.upsert_order(mapped)
            processed += 1
            mod_at = order.get("ModifiedAt")
            if mod_at:
                latest_modified_at = str(mod_at)
        except Exception as e:
            failed += 1
            logger.error("Failed to sync order %s: %s", order.get("OrderId"), e)
            if _failed_record_repo:
                try:
                    _failed_record_repo.save_failed_record(
                        job_name=context.job_name,
                        source_id=str(order.get("OrderId", "unknown")),
                        source_table="HmisOrders",
                        error_message=str(e)[:2000],
                        raw_data=json.dumps(order, default=str)[:4000],
                    )
                except Exception as save_err:
                    logger.error("Failed to save failed record: %s", save_err)

    result.records_processed = processed
    result.records_failed = failed
    if latest_modified_at:
        result.checkpoint_value = latest_modified_at
    if failed > 0 and processed == 0:
        result.status = JobStatus.FAILED

    logger.info("Order sync completed: processed=%d failed=%d", processed, failed)
    return result


def _map_order(hmis_record: dict) -> dict:
    """Map HMIS order fields to DMS order fields."""
    return {
        "HmisOrderId": hmis_record.get("OrderId"),
        "OrderNumber": hmis_record.get("OrderNumber"),
        "CustomerId": hmis_record.get("CustomerId"),
        "DealerId": hmis_record.get("DealerId"),
        "OrderDate": hmis_record.get("OrderDate"),
        "Amount": hmis_record.get("TotalAmount"),
        "Status": hmis_record.get("Status"),
        "HmisModifiedAt": hmis_record.get("ModifiedAt"),
    }
