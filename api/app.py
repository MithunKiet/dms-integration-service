"""FastAPI application factory."""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.middleware import RequestLoggingMiddleware
from api.routers import health, jobs, queue, status

logger = logging.getLogger(__name__)


def create_app(
    queue_service=None,
    job_service=None,
    api_client_service=None,
    api_audit_service=None,
    health_checker=None,
    scheduler_manager=None,
) -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="DMS Integration Service API",
        description="API for on-demand job triggering and status monitoring.",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )
    app.add_middleware(RequestLoggingMiddleware)

    from api.dependencies import set_auth_services
    from api.routers.health import set_health_checker
    from api.routers.jobs import set_services as set_job_services
    from api.routers.queue import set_queue_service
    from api.routers.status import set_scheduler

    if api_client_service and api_audit_service:
        set_auth_services(api_client_service, api_audit_service)
    if health_checker:
        set_health_checker(health_checker)
    if queue_service and job_service:
        set_job_services(queue_service, job_service)
    if queue_service:
        set_queue_service(queue_service)
    if scheduler_manager:
        set_scheduler(scheduler_manager)

    app.include_router(health.router)
    app.include_router(jobs.router)
    app.include_router(queue.router)
    app.include_router(status.router)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.exception(
            "Unhandled exception on %s %s: %s", request.method, request.url.path, exc
        )
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error_code": "INTERNAL_ERROR",
                "message": "Internal server error",
            },
        )

    return app
