"""Service for validating sync records before writing."""
from typing import Any, Dict, List
import logging

logger = logging.getLogger(__name__)


class ValidationResult:
    """Holds the outcome of one or more validation checks."""

    def __init__(self) -> None:
        self.is_valid: bool = True
        self.errors: List[str] = []

    def add_error(self, msg: str) -> None:
        """Record a validation error and mark the result as invalid.

        Args:
            msg: Human-readable description of the validation failure.
        """
        self.is_valid = False
        self.errors.append(msg)


class ValidationService:
    """Provides reusable field-level validation helpers for sync records."""

    def validate_required_fields(
        self,
        record: Dict[str, Any],
        required_fields: List[str],
    ) -> ValidationResult:
        """Check that all *required_fields* are present and non-empty.

        Args:
            record: The data record to validate.
            required_fields: Names of fields that must be non-None and non-empty.

        Returns:
            A :class:`ValidationResult` summarising any missing fields.
        """
        result = ValidationResult()
        for field in required_fields:
            if record.get(field) is None or record.get(field) == "":
                result.add_error(f"Required field missing: {field}")
        return result

    def validate_string_length(
        self,
        record: Dict[str, Any],
        field: str,
        max_len: int,
    ) -> ValidationResult:
        """Check that a string field does not exceed *max_len* characters.

        Args:
            record: The data record to validate.
            field: Name of the field to check.
            max_len: Maximum allowed character length.

        Returns:
            A :class:`ValidationResult` indicating whether the constraint is met.
        """
        result = ValidationResult()
        value = record.get(field)
        if value and len(str(value)) > max_len:
            result.add_error(f"Field '{field}' exceeds max length {max_len}")
        return result

    def merge_results(self, *results: ValidationResult) -> ValidationResult:
        """Combine multiple :class:`ValidationResult` objects into one.

        Args:
            *results: Any number of :class:`ValidationResult` instances.

        Returns:
            A single :class:`ValidationResult` that is invalid if any input
            was invalid, with all error messages aggregated.
        """
        merged = ValidationResult()
        for r in results:
            if not r.is_valid:
                merged.is_valid = False
                merged.errors.extend(r.errors)
        return merged
