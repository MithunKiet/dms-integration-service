"""Queue status router."""
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_authenticated_client
from api.schemas.queue_item_response import QueueItemResponse, QueueListResponse
from models.api_client import ApiClient

router = APIRouter(prefix="/api/queue", tags=["Queue"])

_queue_service = None


def set_queue_service(svc) -> None:
    """Wire the queue service at application startup."""
    global _queue_service
    _queue_service = svc


@router.get("", response_model=QueueListResponse)
async def list_queue(
    limit: int = 50,
    client: ApiClient = Depends(get_authenticated_client),
) -> QueueListResponse:
    """List recent queue items."""
    if _queue_service is None:
        raise HTTPException(status_code=503, detail="Queue service unavailable")

    items = _queue_service.list_items(limit=limit)
    response_items = [
        QueueItemResponse(
            queue_id=item.get("QueueId", 0),
            job_name=item.get("JobName", ""),
            status=item.get("Status", "pending"),
            priority=item.get("Priority", 5),
            requested_by=item.get("RequestedBy"),
            created_at=item.get("CreatedAt"),
            picked_at=item.get("PickedAt"),
            completed_at=item.get("CompletedAt"),
            error_message=item.get("ErrorMessage"),
        )
        for item in items
    ]
    return QueueListResponse(items=response_items, total=len(response_items))
