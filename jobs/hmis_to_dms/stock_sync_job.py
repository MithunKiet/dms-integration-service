"""HMIS to DMS stock synchronization job."""
import json
import logging
from typing import Optional

from config.constants import DEFAULT_BATCH_SIZE
from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_hmis_stock_repo = None
_dms_stock_repo = None
_failed_record_repo = None


def set_dependencies(hmis_stock_repo, dms_stock_repo, failed_record_repo) -> None:
    """Inject repositories at startup."""
    global _hmis_stock_repo, _dms_stock_repo, _failed_record_repo
    _hmis_stock_repo = hmis_stock_repo
    _dms_stock_repo = dms_stock_repo
    _failed_record_repo = failed_record_repo


def run_hmis_to_dms_stock_sync(context: JobContext) -> JobResult:
    """Sync stock records from HMIS to DMS using incremental watermark strategy."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if not _hmis_stock_repo or not _dms_stock_repo:
        logger.warning("stock_sync_job: repositories not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    last_sync = context.checkpoint.last_sync_value if context.checkpoint else None
    logger.info("Stock sync starting. Last checkpoint: %s", last_sync)

    stocks = _hmis_stock_repo.get_stocks_since(last_sync, batch_size=DEFAULT_BATCH_SIZE)
    result.records_read = len(stocks)

    if not stocks:
        logger.info("No new stock records to sync.")
        return result

    processed = 0
    failed = 0
    latest_modified_at: Optional[str] = None

    for stock in stocks:
        try:
            mapped = _map_stock(stock)
            if not mapped.get("StockId"):
                raise ValueError("Missing StockId in mapped stock")
            _dms_stock_repo.upsert_stock(mapped)
            processed += 1
            mod_at = stock.get("ModifiedAt")
            if mod_at:
                latest_modified_at = str(mod_at)
        except Exception as e:
            failed += 1
            logger.error("Failed to sync stock %s: %s", stock.get("StockId"), e)
            if _failed_record_repo:
                try:
                    _failed_record_repo.save_failed_record(
                        job_name=context.job_name,
                        source_id=str(stock.get("StockId", "unknown")),
                        source_table="HmisStock",
                        error_message=str(e)[:2000],
                        raw_data=json.dumps(stock, default=str)[:4000],
                    )
                except Exception as save_err:
                    logger.error("Failed to save failed record: %s", save_err)

    result.records_processed = processed
    result.records_failed = failed
    if latest_modified_at:
        result.checkpoint_value = latest_modified_at
    if failed > 0 and processed == 0:
        result.status = JobStatus.FAILED

    logger.info("Stock sync completed: processed=%d failed=%d", processed, failed)
    return result


def _map_stock(hmis_record: dict) -> dict:
    """Map HMIS stock fields to DMS stock fields."""
    return {
        "StockId": hmis_record.get("StockId"),
        "ProductId": hmis_record.get("ProductId"),
        "DealerId": hmis_record.get("DealerId"),
        "Quantity": hmis_record.get("QuantityOnHand"),
        "ReorderLevel": hmis_record.get("ReorderLevel"),
        "Warehouse": hmis_record.get("WarehouseCode"),
        "HmisModifiedAt": hmis_record.get("ModifiedAt"),
    }
