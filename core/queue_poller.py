"""Background queue poller that picks up on-demand job requests."""
from __future__ import annotations

import threading
import logging
import time
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from services.queue_service import QueueService

logger = logging.getLogger(__name__)


class QueuePoller:
    """Polls IntegrationJobQueue and dispatches jobs to the job runner.

    Runs in its own daemon thread and spawns a new worker thread for each
    queue item up to *max_concurrent* simultaneous jobs.

    Args:
        queue_service: Service used to read and update queue items.
        job_executor: Callable invoked with ``(job_name, payload)`` to run a job.
        poll_interval: Seconds between queue poll cycles.
        max_concurrent: Maximum number of jobs that may run simultaneously.
    """

    def __init__(
        self,
        queue_service: "QueueService",
        job_executor: Callable[[str, dict], None],
        poll_interval: int = 10,
        max_concurrent: int = 3,
    ) -> None:
        self._queue_service = queue_service
        self._job_executor = job_executor
        self._poll_interval = poll_interval
        self._max_concurrent = max_concurrent
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._active_jobs: int = 0
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the poller's background thread."""
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="QueuePoller",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "Queue poller started (interval=%ds, max_concurrent=%d)",
            self._poll_interval,
            self._max_concurrent,
        )

    def stop(self) -> None:
        """Signal the poller to stop and wait up to 30 seconds for it to exit."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=30)
        logger.info("Queue poller stopped.")

    def _poll_loop(self) -> None:
        """Main loop executed by the background poller thread."""
        while self._running:
            try:
                self._process_pending()
            except Exception as e:
                logger.error("Queue poller error: %s", e)
            time.sleep(self._poll_interval)

    def _process_pending(self) -> None:
        """Fetch pending items and dispatch them to worker threads."""
        with self._lock:
            available_slots = self._max_concurrent - self._active_jobs

        if available_slots <= 0:
            return

        items = self._queue_service.get_pending(limit=available_slots)

        for item in items:
            with self._lock:
                if self._active_jobs >= self._max_concurrent:
                    break
                self._active_jobs += 1

            try:
                self._queue_service.mark_running(item.queue_id)
            except Exception as e:
                logger.error(
                    "Failed to mark queue item %d as running: %s", item.queue_id, e
                )
                with self._lock:
                    self._active_jobs -= 1
                continue

            payload = item.payload or {}
            thread = threading.Thread(
                target=self._execute_job,
                args=(item.queue_id, item.job_name, payload),
                name=f"Job-{item.job_name}-{item.queue_id}",
                daemon=True,
            )
            thread.start()

    def _execute_job(self, queue_id: int, job_name: str, payload: dict) -> None:
        """Run a single job and update its queue status when finished.

        Args:
            queue_id: Primary key of the queue item being executed.
            job_name: Name of the job to run.
            payload: Parsed payload dict for the job.
        """
        try:
            logger.info(
                "Executing queued job: job_name='%s' queue_id=%d", job_name, queue_id
            )
            self._job_executor(job_name, payload)
            self._queue_service.mark_completed(queue_id)
        except Exception as e:
            logger.error(
                "Queued job failed: job_name='%s' queue_id=%d error=%s",
                job_name,
                queue_id,
                e,
            )
            try:
                self._queue_service.mark_failed(queue_id, str(e)[:2000])
            except Exception:
                pass
        finally:
            with self._lock:
                self._active_jobs -= 1
