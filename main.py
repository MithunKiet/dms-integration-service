"""Application entry point — composition root.

Builds all database connections, repositories, services, and the scheduler,
then wires them into the FastAPI app and starts Uvicorn.
"""
from __future__ import annotations

import logging
import sys
from typing import Optional

import uvicorn
from pyodbc import Connection

from config.logging_config import setup_logging
from config.settings import Settings
from core.db import DatabaseManager
from core.scheduler import SchedulerManager
from core.health_adapter import BoundHealthChecker

from repositories.integration.queue_repository import QueueRepository
from repositories.integration.job_repository import JobRepository
from repositories.integration.job_log_repository import JobLogRepository
from repositories.integration.api_client_repository import ApiClientRepository
from repositories.integration.api_audit_repository import ApiAuditRepository

from services.queue_service import QueueService
from services.job_service import JobService
from services.api_client_service import ApiClientService
from services.api_audit_service import ApiAuditService

from api.app import create_app

logger = logging.getLogger(__name__)


def build_app():
    """Compose all dependencies and return a configured FastAPI app."""
    settings = Settings.get_settings()

    # ── Logging ────────────────────────────────────────────────────────────
    setup_logging(log_level=settings.log_level, log_dir=settings.log_dir)
    logger.info("Starting %s [%s]", settings.app_name, settings.app_env)

    # ── Database ───────────────────────────────────────────────────────────
    db_manager = DatabaseManager(
        hmis_connection_string=settings.hmis_connection,
        dms_connection_string=settings.dms_connection,
        integration_connection_string=settings.integration_connection,
    )

    # Integration connection is used by all integration repositories.
    integration_conn: Optional[Connection]
    try:
        integration_conn = db_manager.get_integration_connection()
    except Exception as exc:
        logger.warning(
            "Could not open integration DB connection at startup: %s — "
            "the app will start but DB-backed endpoints will fail.",
            exc,
        )
        integration_conn = None

    # ── Repositories ───────────────────────────────────────────────────────
    queue_repo: Optional[QueueRepository]
    job_repo: Optional[JobRepository]
    job_log_repo: Optional[JobLogRepository]
    api_client_repo: Optional[ApiClientRepository]
    api_audit_repo: Optional[ApiAuditRepository]

    if integration_conn is not None:
        queue_repo = QueueRepository(integration_conn)
        job_repo = JobRepository(integration_conn)
        job_log_repo = JobLogRepository(integration_conn)
        api_client_repo = ApiClientRepository(integration_conn)
        api_audit_repo = ApiAuditRepository(integration_conn)
    else:
        queue_repo = None
        job_repo = None
        job_log_repo = None
        api_client_repo = None
        api_audit_repo = None

    # ── Services ───────────────────────────────────────────────────────────
    queue_service = QueueService(queue_repo) if queue_repo else None
    job_service = JobService(job_repo, job_log_repo) if job_repo and job_log_repo else None
    api_client_service = ApiClientService(
        client_repo=api_client_repo,
        configured_clients=settings.api_key_settings.clients,
    )
    api_audit_service = ApiAuditService(api_audit_repo) if api_audit_repo else None

    # ── Scheduler ──────────────────────────────────────────────────────────
    scheduler = SchedulerManager()
    scheduler.start()
    logger.info("Scheduler started.")

    # ── Health checker (adapter exposes no-arg get_system_health) ──────────
    health_checker = BoundHealthChecker(
        db_manager=db_manager,
        scheduler=scheduler.get_scheduler(),
        queue_repo=queue_repo,
        include_integration_db=False,
    )

    # ── FastAPI app ────────────────────────────────────────────────────────
    app = create_app(
        queue_service=queue_service,
        job_service=job_service,
        api_client_service=api_client_service,
        api_audit_service=api_audit_service,
        health_checker=health_checker,
        scheduler_manager=scheduler,
    )

    logger.info(
        "App wired — health_checker=%s  queue=%s  jobs=%s  auth=%s  scheduler=running",
        health_checker is not None,
        queue_service is not None,
        job_service is not None,
        api_client_service is not None,
    )
    return app


# Module-level app instance so Uvicorn can import it as `main:app`.
# When this file is executed directly, avoid eager construction because
# `uvicorn.run("main:app", ...)` will import this module again.
app = build_app() if __name__ != "__main__" else None


if __name__ == "__main__":
    settings = Settings.get_settings()

    # PyCharm/Windows debug works best without auto-reload or multi-workers.
    debug_session = sys.gettrace() is not None
    if debug_session:
        logger.info(
            "Debugger detected: forcing single-process Uvicorn "
            "(workers=1, reload=False)."
        )

    uvicorn.run(
        build_app() if debug_session else "main:app",
        host=settings.api_host,
        port=settings.api_port,
        workers=1 if debug_session else settings.api_workers,
        log_level=settings.log_level.lower(),
        reload=False if debug_session else settings.debug,
    )

