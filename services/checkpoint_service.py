"""Service for managing job sync checkpoints."""
from typing import Optional
import logging

from repositories.integration.checkpoint_repository import CheckpointRepository
from models.checkpoint import Checkpoint

logger = logging.getLogger(__name__)


class CheckpointService:
    """Manages reading and writing job synchronisation checkpoints."""

    def __init__(self, checkpoint_repo: CheckpointRepository) -> None:
        self._repo = checkpoint_repo

    def get_checkpoint(self, job_name: str) -> Checkpoint:
        """Retrieve the current checkpoint for a job.

        Returns a default empty :class:`~models.checkpoint.Checkpoint` when no
        checkpoint has been persisted yet (e.g. first run).

        Args:
            job_name: Name of the job whose checkpoint to retrieve.

        Returns:
            A :class:`~models.checkpoint.Checkpoint` instance.
        """
        row = self._repo.get_checkpoint(job_name)
        if row:
            return Checkpoint(
                job_name=job_name,
                last_sync_value=row.get("LastSyncValue"),
                last_sync_at=row.get("LastSyncAt"),
                extra_state=row.get("ExtraState"),
            )
        return Checkpoint(job_name=job_name)

    def update_checkpoint(
        self,
        job_name: str,
        last_sync_value: str,
        extra_state: Optional[str] = None,
    ) -> None:
        """Persist an updated checkpoint for a job.

        Args:
            job_name: Name of the job whose checkpoint should be updated.
            last_sync_value: The new cursor value (e.g. ISO timestamp, row ID).
            extra_state: Optional serialised JSON for additional job state.
        """
        self._repo.upsert_checkpoint(job_name, last_sync_value, extra_state)
        logger.info("Checkpoint updated for job '%s': %s", job_name, last_sync_value)
