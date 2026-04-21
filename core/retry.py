"""Retry-with-backoff utility for resilient operation execution."""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable, Tuple, Type

from core.exceptions import RetryExhaustedError

logger = logging.getLogger(__name__)


def retry_with_backoff(
    func: Callable[..., Any],
    *args: Any,
    max_attempts: int = 3,
    backoff_seconds: float = 30.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    **kwargs: Any,
) -> Any:
    """Call *func* with exponential back-off, retrying on specified exceptions.

    Each successive retry waits for ``backoff_seconds * 2^(attempt-1)`` seconds
    plus a random jitter of up to 10 % of the base delay to avoid thundering-herd
    problems.

    Args:
        func: The callable to invoke.
        *args: Positional arguments forwarded to *func*.
        max_attempts: Total number of attempts before giving up.
        backoff_seconds: Base delay (in seconds) between retries.
        exceptions: Tuple of exception types that should trigger a retry.
            Any exception NOT in this tuple will propagate immediately.
        **kwargs: Keyword arguments forwarded to *func*.

    Returns:
        The return value of *func* on a successful invocation.

    Raises:
        :class:`~core.exceptions.RetryExhaustedError`: When all *max_attempts*
            have been exhausted without a successful call.
        Any exception not listed in *exceptions* propagates immediately.
    """
    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except exceptions as exc:
            last_exc = exc
            if attempt == max_attempts:
                break

            base_delay = backoff_seconds * (2 ** (attempt - 1))
            jitter = random.uniform(0, base_delay * 0.1)
            delay = base_delay + jitter

            logger.warning(
                "Attempt %d/%d for '%s' failed: %s. Retrying in %.1fs.",
                attempt,
                max_attempts,
                getattr(func, "__name__", repr(func)),
                exc,
                delay,
            )
            time.sleep(delay)

    raise RetryExhaustedError(
        f"All {max_attempts} attempt(s) for "
        f"'{getattr(func, '__name__', repr(func))}' failed. "
        f"Last error: {last_exc}"
    ) from last_exc
