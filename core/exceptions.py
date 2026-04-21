"""Custom exception hierarchy for the DMS Integration Service."""

from __future__ import annotations


class DMSIntegrationError(Exception):
    """Base exception for all DMS Integration Service errors.

    All application-specific exceptions inherit from this class so that
    callers can catch them with a single ``except DMSIntegrationError`` clause.
    """


class DatabaseConnectionError(DMSIntegrationError):
    """Raised when a database connection cannot be established or is lost."""


class JobExecutionError(DMSIntegrationError):
    """Raised when a scheduled or on-demand job fails during execution."""


class JobAlreadyRunningError(DMSIntegrationError):
    """Raised when a job cannot start because a lock is currently held."""


class CheckpointError(DMSIntegrationError):
    """Raised when reading or writing a job checkpoint fails."""


class ValidationError(DMSIntegrationError):
    """Raised when source data fails validation rules before mapping."""


class MappingError(DMSIntegrationError):
    """Raised when a data transformation or field mapping cannot be applied."""


class QueueError(DMSIntegrationError):
    """Raised when an operation on the on-demand job queue fails."""


class AuthenticationError(DMSIntegrationError):
    """Raised when an API client cannot be authenticated."""


class AuthorizationError(DMSIntegrationError):
    """Raised when an authenticated client is not permitted to perform an action."""


class ConfigurationError(DMSIntegrationError):
    """Raised when required configuration values are missing or invalid."""


class RetryExhaustedError(DMSIntegrationError):
    """Raised when all retry attempts for an operation have been exhausted."""
