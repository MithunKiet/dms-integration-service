"""Status router for service-level information."""
from fastapi import APIRouter, Depends

from api.dependencies import get_authenticated_client
from models.api_client import ApiClient

router = APIRouter(prefix="/api/status", tags=["Status"])

_scheduler_manager = None


def set_scheduler(mgr) -> None:
    """Wire the scheduler manager at application startup."""
    global _scheduler_manager
    _scheduler_manager = mgr


@router.get("")
async def get_service_status(
    client: ApiClient = Depends(get_authenticated_client),
) -> dict:
    """Get scheduler and service status."""
    jobs = []
    if _scheduler_manager:
        for job in _scheduler_manager.get_jobs():
            jobs.append(
                {
                    "id": job.id,
                    "name": job.name,
                    "next_run_time": (
                        str(job.next_run_time) if job.next_run_time else None
                    ),
                }
            )
    return {
        "scheduler_running": (
            _scheduler_manager.is_running if _scheduler_manager else False
        ),
        "scheduled_jobs": jobs,
    }
