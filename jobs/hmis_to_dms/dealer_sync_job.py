"""HMIS to DMS dealer synchronization job."""
import json
import logging
from typing import Optional

from config.constants import DEFAULT_BATCH_SIZE
from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_hmis_dealer_repo = None
_dms_dealer_repo = None
_failed_record_repo = None


def set_dependencies(hmis_dealer_repo, dms_dealer_repo, failed_record_repo) -> None:
    """Inject repositories at startup."""
    global _hmis_dealer_repo, _dms_dealer_repo, _failed_record_repo
    _hmis_dealer_repo = hmis_dealer_repo
    _dms_dealer_repo = dms_dealer_repo
    _failed_record_repo = failed_record_repo


def run_hmis_to_dms_dealer_sync(context: JobContext) -> JobResult:
    """Sync dealers from HMIS to DMS using incremental watermark strategy."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if not _hmis_dealer_repo or not _dms_dealer_repo:
        logger.warning("dealer_sync_job: repositories not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    last_sync = context.checkpoint.last_sync_value if context.checkpoint else None
    logger.info("Dealer sync starting. Last checkpoint: %s", last_sync)

    dealers = _hmis_dealer_repo.get_dealers_since(
        last_sync, batch_size=DEFAULT_BATCH_SIZE
    )
    result.records_read = len(dealers)

    if not dealers:
        logger.info("No new dealers to sync.")
        return result

    processed = 0
    failed = 0
    latest_modified_at: Optional[str] = None

    for dealer in dealers:
        try:
            mapped = _map_dealer(dealer)
            if not mapped.get("DealerId"):
                raise ValueError("Missing DealerId in mapped dealer")
            _dms_dealer_repo.upsert_dealer(mapped)
            processed += 1
            mod_at = dealer.get("ModifiedAt")
            if mod_at:
                latest_modified_at = str(mod_at)
        except Exception as e:
            failed += 1
            logger.error("Failed to sync dealer %s: %s", dealer.get("DealerId"), e)
            if _failed_record_repo:
                try:
                    _failed_record_repo.save_failed_record(
                        job_name=context.job_name,
                        source_id=str(dealer.get("DealerId", "unknown")),
                        source_table="HmisDealers",
                        error_message=str(e)[:2000],
                        raw_data=json.dumps(dealer, default=str)[:4000],
                    )
                except Exception as save_err:
                    logger.error("Failed to save failed record: %s", save_err)

    result.records_processed = processed
    result.records_failed = failed
    if latest_modified_at:
        result.checkpoint_value = latest_modified_at
    if failed > 0 and processed == 0:
        result.status = JobStatus.FAILED

    logger.info("Dealer sync completed: processed=%d failed=%d", processed, failed)
    return result


def _map_dealer(hmis_record: dict) -> dict:
    """Map HMIS dealer fields to DMS dealer fields."""
    return {
        "DealerId": hmis_record.get("DealerId"),
        "DealerName": hmis_record.get("DealerName"),
        "DealerCode": hmis_record.get("DealerCode"),
        "Region": hmis_record.get("RegionCode"),
        "Phone": hmis_record.get("ContactPhone"),
        "Email": hmis_record.get("ContactEmail"),
        "Address": hmis_record.get("Address"),
        "IsActive": hmis_record.get("IsActive", True),
        "HmisModifiedAt": hmis_record.get("ModifiedAt"),
    }
