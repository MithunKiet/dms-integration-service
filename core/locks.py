"""Distributed job-lock manager backed by the INTEGRATION database."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from config.constants import TABLE_JOB_LOCK
from core.exceptions import DatabaseConnectionError
from core.db import DatabaseManager

logger = logging.getLogger(__name__)

_ACQUIRE_SQL = f"""
    IF NOT EXISTS (
        SELECT 1 FROM {TABLE_JOB_LOCK}
        WHERE job_name = ?
          AND locked = 1
          AND locked_at > DATEADD(MINUTE, -?, GETUTCDATE())
    )
    BEGIN
        DELETE FROM {TABLE_JOB_LOCK} WHERE job_name = ?;
        INSERT INTO {TABLE_JOB_LOCK} (job_name, locked, locked_at)
        VALUES (?, 1, GETUTCDATE());
        SELECT 1 AS acquired;
    END
    ELSE
    BEGIN
        SELECT 0 AS acquired;
    END
"""

_RELEASE_SQL = f"""
    DELETE FROM {TABLE_JOB_LOCK} WHERE job_name = ?;
"""

_IS_LOCKED_SQL = f"""
    SELECT COUNT(1) FROM {TABLE_JOB_LOCK}
    WHERE job_name = ?
      AND locked = 1
      AND locked_at > DATEADD(MINUTE, -?, GETUTCDATE());
"""


class LockManager:
    """Manages per-job distributed locks stored in the INTEGRATION database.

    Locks expire automatically after *timeout_minutes* to prevent stale entries
    from blocking jobs indefinitely (e.g. after a crashed worker).

    Args:
        db_manager: A :class:`~core.db.DatabaseManager` instance whose
            integration connection will be used.
        default_timeout_minutes: Default lock expiry in minutes.
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        default_timeout_minutes: int = 60,
    ) -> None:
        self._db = db_manager
        self._default_timeout = default_timeout_minutes

    def acquire_lock(
        self, job_name: str, timeout_minutes: int | None = None
    ) -> bool:
        """Attempt to acquire an exclusive lock for *job_name*.

        The lock is inserted into the database only when no unexpired lock
        for the same job exists.  Stale locks (older than *timeout_minutes*)
        are deleted before the insert so that crashed workers do not block
        subsequent runs.

        Args:
            job_name: Unique name identifying the job.
            timeout_minutes: How long the lock is valid.  Defaults to the
                value supplied at construction time.

        Returns:
            ``True`` if the lock was successfully acquired, ``False`` if
            another worker currently holds it.
        """
        timeout = timeout_minutes if timeout_minutes is not None else self._default_timeout
        try:
            conn = self._db.get_integration_connection()
            rows = self._db.execute_query(
                conn,
                _ACQUIRE_SQL,
                (timeout, timeout, job_name, job_name),
            )
            conn.commit()
            acquired = bool(rows and rows[0][0])
            if acquired:
                logger.debug("Lock acquired for job '%s'.", job_name)
            else:
                logger.debug("Lock NOT acquired for job '%s' (already locked).", job_name)
            return acquired
        except Exception as exc:
            logger.error("Error acquiring lock for '%s': %s", job_name, exc)
            return False

    def release_lock(self, job_name: str) -> None:
        """Release the lock held for *job_name*.

        Args:
            job_name: Unique name identifying the job whose lock should be freed.
        """
        try:
            conn = self._db.get_integration_connection()
            self._db.execute_non_query(conn, _RELEASE_SQL, (job_name,))
            conn.commit()
            logger.debug("Lock released for job '%s'.", job_name)
        except Exception as exc:
            logger.error("Error releasing lock for '%s': %s", job_name, exc)

    def is_locked(self, job_name: str, timeout_minutes: int | None = None) -> bool:
        """Check whether an active (non-expired) lock exists for *job_name*.

        Args:
            job_name: Unique name identifying the job.
            timeout_minutes: Expiry window to apply when checking.  Defaults to
                the value supplied at construction time.

        Returns:
            ``True`` if the job is currently locked, ``False`` otherwise.
        """
        timeout = timeout_minutes if timeout_minutes is not None else self._default_timeout
        try:
            conn = self._db.get_integration_connection()
            rows = self._db.execute_query(conn, _IS_LOCKED_SQL, (job_name, timeout))
            return bool(rows and rows[0][0] > 0)
        except Exception as exc:
            logger.error("Error checking lock for '%s': %s", job_name, exc)
            return False
