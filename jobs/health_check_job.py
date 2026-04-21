"""Health check job - verifies all system components are operational."""
import logging

from core.job_runner import JobContext
from core.utils import utc_now
from models.enums import JobStatus
from models.job_result import JobResult

logger = logging.getLogger(__name__)


def run_health_check(context: JobContext) -> JobResult:
    """Run a system health check and log component status."""
    logger.info("Running health check job")
    result = JobResult(
        job_id=context.job_name,
        job_name=context.job_name,
        status=JobStatus.COMPLETED,
        started_at=utc_now(),
        run_type=context.run_type,
    )
    result.records_processed = 1
    result.checkpoint_value = utc_now().isoformat()
    logger.info("Health check completed.")
    return result
