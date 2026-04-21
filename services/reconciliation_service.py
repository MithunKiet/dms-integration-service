"""Reconciliation service for detecting and resolving data discrepancies."""
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ReconciliationService:
    """Compares source and target record sets to surface discrepancies."""

    def compare_records(
        self,
        source_records: List[Dict[str, Any]],
        target_records: List[Dict[str, Any]],
        key_field: str,
    ) -> Dict[str, Any]:
        """Identify records present in one dataset but missing from the other.

        Both lists are indexed by ``str(record[key_field])``, so the key field
        must be unique within each dataset.

        Args:
            source_records: Records from the authoritative source system.
            target_records: Records from the target system to verify.
            key_field: The field name used as the primary key for comparison.

        Returns:
            A dict with the following keys:

            * ``missing_in_target``: Records in *source_records* not found in
              *target_records*.
            * ``extra_in_target``: Records in *target_records* not found in
              *source_records*.
            * ``source_count``: Total number of source records provided.
            * ``target_count``: Total number of target records provided.
        """
        source_map = {str(r[key_field]): r for r in source_records}
        target_map = {str(r[key_field]): r for r in target_records}

        missing_in_target = [source_map[k] for k in source_map if k not in target_map]
        extra_in_target = [target_map[k] for k in target_map if k not in source_map]

        return {
            "missing_in_target": missing_in_target,
            "extra_in_target": extra_in_target,
            "source_count": len(source_records),
            "target_count": len(target_records),
        }
