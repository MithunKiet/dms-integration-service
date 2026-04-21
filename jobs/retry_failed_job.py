"""Retry failed records job - reprocesses previously failed sync records."""
import logging

from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)

_failed_record_repo = None
_sync_dispatcher = None


def set_dependencies(failed_record_repo, sync_dispatcher=None) -> None:
    """Inject repository and optional dispatcher at startup."""
    global _failed_record_repo, _sync_dispatcher
    _failed_record_repo = failed_record_repo
    _sync_dispatcher = sync_dispatcher


def run_retry_failed(context: JobContext) -> JobResult:
    """Retry all unresolved failed records."""
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )

    if _failed_record_repo is None:
        logger.warning("retry_failed_job: failed_record_repo not set, skipping.")
        result.status = JobStatus.SKIPPED
        return result

    processed = 0
    failed = 0

    from config.constants import ALL_JOB_NAMES

    for job_name in ALL_JOB_NAMES:
        records = _failed_record_repo.get_unresolved_by_job(job_name, limit=100)
        for record in records:
            try:
                _failed_record_repo.increment_retry(record.get("Id", 0))
                processed += 1
            except Exception as e:
                logger.error("Failed to retry record %s: %s", record.get("Id"), e)
                failed += 1

    result.records_read = processed + failed
    result.records_processed = processed
    result.records_failed = failed
    result.checkpoint_value = utc_now().isoformat()
    return result
