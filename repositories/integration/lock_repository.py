"""Repository for JobLock table operations."""
import logging

from repositories.base_repository import BaseRepository
from config.constants import TABLE_JOB_LOCK

logger = logging.getLogger(__name__)


class LockRepository(BaseRepository):
    """Provides distributed-lock operations against the ``JobLock`` table.

    Locks expire automatically after *timeout_minutes* to prevent stale entries
    from blocking jobs after a crashed worker.
    """

    def acquire(self, job_name: str, timeout_minutes: int = 60) -> bool:
        """Attempt to acquire an exclusive lock for *job_name*.

        Stale locks older than *timeout_minutes* are deleted before the insert,
        so a crashed worker will not block future runs indefinitely.

        Args:
            job_name: Unique identifier for the job.
            timeout_minutes: How long until the lock is considered stale.

        Returns:
            ``True`` if the lock was acquired, ``False`` if another worker holds it.
        """
        sql = f"""
            IF NOT EXISTS (
                SELECT 1 FROM {TABLE_JOB_LOCK}
                 WHERE job_name  = ?
                   AND locked    = 1
                   AND locked_at > DATEADD(MINUTE, -?, GETUTCDATE())
            )
            BEGIN
                DELETE FROM {TABLE_JOB_LOCK} WHERE job_name = ?;
                INSERT INTO {TABLE_JOB_LOCK} (job_name, locked, locked_at)
                VALUES (?, 1, GETUTCDATE());
                SELECT 1 AS Acquired;
            END
            ELSE
            BEGIN
                SELECT 0 AS Acquired;
            END
        """
        try:
            result = self.execute_scalar(
                sql,
                (job_name, timeout_minutes, job_name, job_name),
            )
            acquired = bool(result)
            logger.debug("Lock acquire for '%s': %s", job_name, acquired)
            return acquired
        except Exception as e:
            logger.error("Error acquiring lock for '%s': %s", job_name, e)
            return False

    def release(self, job_name: str) -> None:
        """Release the lock held for *job_name*.

        Args:
            job_name: Unique identifier for the job whose lock should be freed.
        """
        sql = f"DELETE FROM {TABLE_JOB_LOCK} WHERE job_name = ?"
        try:
            self.execute_non_query(sql, (job_name,))
            logger.debug("Lock released for '%s'.", job_name)
        except Exception as e:
            logger.error("Error releasing lock for '%s': %s", job_name, e)

    def is_locked(self, job_name: str) -> bool:
        """Check whether an active (non-expired) lock exists for *job_name*.

        Uses a default expiry of 60 minutes when evaluating staleness.

        Args:
            job_name: Unique identifier for the job.

        Returns:
            ``True`` if a valid lock exists, ``False`` otherwise.
        """
        sql = f"""
            SELECT COUNT(1)
              FROM {TABLE_JOB_LOCK}
             WHERE job_name  = ?
               AND locked    = 1
               AND locked_at > DATEADD(MINUTE, -60, GETUTCDATE())
        """
        try:
            count = self.execute_scalar(sql, (job_name,))
            return bool(count and count > 0)
        except Exception as e:
            logger.error("Error checking lock for '%s': %s", job_name, e)
            return False

    def cleanup_stale_locks(self) -> int:
        """Delete all lock entries that have exceeded the 60-minute expiry.

        Returns:
            The number of stale lock rows removed.
        """
        sql = f"""
            DELETE FROM {TABLE_JOB_LOCK}
             WHERE locked_at <= DATEADD(MINUTE, -60, GETUTCDATE())
        """
        try:
            count = self.execute_non_query(sql)
            logger.info("Cleaned up %d stale lock(s).", count)
            return count
        except Exception as e:
            logger.error("Error cleaning stale locks: %s", e)
            return 0
