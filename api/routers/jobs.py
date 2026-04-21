"""Jobs router - trigger and status endpoints."""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from api.dependencies import get_authenticated_client
from api.schemas.job_status_response import JobLogResponse, JobStatusResponse
from api.schemas.job_trigger_request import JobTriggerRequest
from api.schemas.job_trigger_response import JobTriggerResponse
from models.api_client import ApiClient
from models.enums import QueueStatus

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])

_queue_service = None
_job_service = None


def set_services(queue_svc, job_svc) -> None:
    """Wire queue and job services at application startup."""
    global _queue_service, _job_service
    _queue_service = queue_svc
    _job_service = job_svc


@router.post("/trigger", response_model=JobTriggerResponse, status_code=202)
async def trigger_job(
    request: JobTriggerRequest,
    client: ApiClient = Depends(get_authenticated_client),
) -> JobTriggerResponse:
    """Enqueue a job for on-demand execution. Returns 202 Accepted."""
    if _queue_service is None:
        raise HTTPException(status_code=503, detail="Queue service unavailable")

    queue_id = _queue_service.enqueue(
        job_name=request.job_name,
        requested_by=client.client_id,
        payload=request.payload,
        priority=request.priority,
    )
    return JobTriggerResponse(
        queue_id=queue_id,
        job_name=request.job_name,
        status=QueueStatus.PENDING,
    )


@router.get("/{job_name}", response_model=JobStatusResponse)
async def get_job_status(
    job_name: str,
    client: ApiClient = Depends(get_authenticated_client),
) -> JobStatusResponse:
    """Get job metadata and recent execution logs."""
    if _job_service is None:
        raise HTTPException(status_code=503, detail="Job service unavailable")

    job = _job_service.get_job(job_name)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_name}' not found")

    logs = _job_service.get_job_logs(job_name, limit=10)
    log_responses = [
        JobLogResponse(
            log_id=log.get("LogId", 0),
            job_name=log.get("JobName", job_name),
            status=log.get("Status", ""),
            run_type=log.get("RunType", ""),
            records_read=log.get("RecordsRead", 0),
            records_processed=log.get("RecordsProcessed", 0),
            records_failed=log.get("RecordsFailed", 0),
            started_at=log.get("StartedAt"),
            ended_at=log.get("EndedAt"),
            error_message=log.get("ErrorMessage"),
        )
        for log in logs
    ]
    return JobStatusResponse(
        job_name=job_name,
        is_active=job.get("IsActive", True),
        description=job.get("Description"),
        recent_logs=log_responses,
    )


@router.get("", response_model=List[dict])
async def list_jobs(
    client: ApiClient = Depends(get_authenticated_client),
) -> List[dict]:
    """List all registered integration jobs."""
    if _job_service is None:
        raise HTTPException(status_code=503, detail="Job service unavailable")
    return _job_service.get_all_jobs()
