"""Health check router."""
from fastapi import APIRouter

from api.schemas.health_response import ComponentHealthResponse, HealthResponse
from models.enums import HealthStatus

router = APIRouter(prefix="/api", tags=["Health"])

_health_checker = None


def set_health_checker(checker) -> None:
    """Inject the health checker instance at startup."""
    global _health_checker
    _health_checker = checker


@router.get("/health", response_model=HealthResponse, summary="Health check")
async def health_check() -> HealthResponse:
    """Return overall system health and per-component status."""
    if _health_checker is None:
        return HealthResponse(
            overall_status=HealthStatus.UNHEALTHY,
            components={},
            version="1.0.0",
        )
    health = _health_checker.get_system_health()
    components = {
        name: ComponentHealthResponse(
            name=comp.name,
            status=comp.status,
            message=comp.message,
        )
        for name, comp in health.components.items()
    }
    return HealthResponse(
        overall_status=health.overall_status,
        components=components,
        version=health.version,
    )
