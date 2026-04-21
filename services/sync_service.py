"""Generic synchronization service providing common sync operations."""
from typing import Optional, List, Callable, Tuple
import logging

logger = logging.getLogger(__name__)


class SyncService:
    """Provides common sync pipeline methods reused by individual sync jobs."""

    def batch_process(
        self,
        records: List[dict],
        process_fn: Callable[[dict], bool],
        on_failure: Optional[Callable[[dict, Exception], None]] = None,
    ) -> Tuple[int, int]:
        """Process a batch of records using *process_fn*.

        Each record is passed to *process_fn*.  A truthy return value counts
        as a success; a falsy return or an exception counts as a failure.
        When *on_failure* is provided it is called with the failing record and
        the exception (best-effort — exceptions from *on_failure* itself are
        swallowed to avoid masking the original error).

        Args:
            records: List of source record dicts to process.
            process_fn: Callable that accepts a record dict and returns ``True``
                on success or ``False`` on a soft failure.
            on_failure: Optional callable invoked for each failed record with
                the record dict and the exception as arguments.

        Returns:
            A tuple of ``(processed_count, failed_count)``.
        """
        processed = 0
        failed = 0
        for record in records:
            try:
                if process_fn(record):
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error("Record processing failed: %s", e)
                if on_failure:
                    try:
                        on_failure(record, e)
                    except Exception:
                        pass
        return processed, failed
