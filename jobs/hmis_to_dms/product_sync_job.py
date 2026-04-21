"""HMIS to DMS product synchronization job."""
import json
import logging
from typing import Optional

from config.constants import DEFAULT_BATCH_SIZE
from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_hmis_product_repo = None
_dms_product_repo = None
_failed_record_repo = None


def set_dependencies(hmis_product_repo, dms_product_repo, failed_record_repo) -> None:
    """Inject repositories at startup."""
    global _hmis_product_repo, _dms_product_repo, _failed_record_repo
    _hmis_product_repo = hmis_product_repo
    _dms_product_repo = dms_product_repo
    _failed_record_repo = failed_record_repo


def run_hmis_to_dms_product_sync(context: JobContext) -> JobResult:
    """Sync products from HMIS to DMS using incremental watermark strategy."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if not _hmis_product_repo or not _dms_product_repo:
        logger.warning("product_sync_job: repositories not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    last_sync = context.checkpoint.last_sync_value if context.checkpoint else None
    logger.info("Product sync starting. Last checkpoint: %s", last_sync)

    products = _hmis_product_repo.get_products_since(
        last_sync, batch_size=DEFAULT_BATCH_SIZE
    )
    result.records_read = len(products)

    if not products:
        logger.info("No new products to sync.")
        return result

    processed = 0
    failed = 0
    latest_modified_at: Optional[str] = None

    for product in products:
        try:
            mapped = _map_product(product)
            if not mapped.get("ProductId"):
                raise ValueError("Missing ProductId in mapped product")
            _dms_product_repo.upsert_product(mapped)
            processed += 1
            mod_at = product.get("ModifiedAt")
            if mod_at:
                latest_modified_at = str(mod_at)
        except Exception as e:
            failed += 1
            logger.error("Failed to sync product %s: %s", product.get("ProductId"), e)
            if _failed_record_repo:
                try:
                    _failed_record_repo.save_failed_record(
                        job_name=context.job_name,
                        source_id=str(product.get("ProductId", "unknown")),
                        source_table="HmisProducts",
                        error_message=str(e)[:2000],
                        raw_data=json.dumps(product, default=str)[:4000],
                    )
                except Exception as save_err:
                    logger.error("Failed to save failed record: %s", save_err)

    result.records_processed = processed
    result.records_failed = failed
    if latest_modified_at:
        result.checkpoint_value = latest_modified_at
    if failed > 0 and processed == 0:
        result.status = JobStatus.FAILED

    logger.info("Product sync completed: processed=%d failed=%d", processed, failed)
    return result


def _map_product(hmis_record: dict) -> dict:
    """Map HMIS product fields to DMS product fields."""
    return {
        "ProductId": hmis_record.get("ProductId"),
        "ProductName": hmis_record.get("ProductName"),
        "SKU": hmis_record.get("SKU"),
        "Category": hmis_record.get("Category"),
        "Price": hmis_record.get("UnitPrice"),
        "IsActive": hmis_record.get("IsActive", True),
        "HmisModifiedAt": hmis_record.get("ModifiedAt"),
    }
