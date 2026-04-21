"""Base repository with common DB operations."""
from abc import ABC
from typing import Any, List, Optional, Tuple
import logging
import pyodbc

logger = logging.getLogger(__name__)


class BaseRepository(ABC):
    """Abstract base repository providing common database operations."""

    def __init__(self, connection: pyodbc.Connection) -> None:
        self._conn = connection

    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[pyodbc.Row]:
        """Execute a SELECT query and return rows.

        Args:
            sql: The SQL SELECT statement to execute.
            params: Optional tuple of bind parameters.

        Returns:
            A list of :class:`pyodbc.Row` objects.
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, params or ())
            return cursor.fetchall()
        except pyodbc.Error as e:
            logger.error("Query execution failed: %s | SQL: %s", e, sql[:200])
            raise

    def execute_scalar(self, sql: str, params: Optional[Tuple] = None) -> Any:
        """Execute a query and return a single scalar value.

        Args:
            sql: The SQL statement to execute.
            params: Optional tuple of bind parameters.

        Returns:
            The first column of the first row, or ``None`` if no rows returned.
        """
        rows = self.execute_query(sql, params)
        if rows:
            return rows[0][0]
        return None

    def execute_non_query(
        self,
        sql: str,
        params: Optional[Tuple] = None,
        commit: bool = True,
    ) -> int:
        """Execute an INSERT/UPDATE/DELETE statement.

        Args:
            sql: The SQL DML statement to execute.
            params: Optional tuple of bind parameters.
            commit: Whether to commit the transaction immediately.

        Returns:
            The number of rows affected.
        """
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, params or ())
            if commit:
                self._conn.commit()
            return cursor.rowcount
        except pyodbc.Error as e:
            self._conn.rollback()
            logger.error("Non-query execution failed: %s | SQL: %s", e, sql[:200])
            raise

    def execute_many(
        self,
        sql: str,
        params_list: List[Tuple],
        commit: bool = True,
    ) -> int:
        """Execute a batch INSERT/UPDATE.

        Args:
            sql: The SQL DML statement to execute for each parameter set.
            params_list: A list of parameter tuples.
            commit: Whether to commit the transaction immediately.

        Returns:
            The rowcount from the final execution.
        """
        try:
            cursor = self._conn.cursor()
            cursor.executemany(sql, params_list)
            if commit:
                self._conn.commit()
            return cursor.rowcount
        except pyodbc.Error as e:
            self._conn.rollback()
            logger.error("Batch execution failed: %s | SQL: %s", e, sql[:200])
            raise

    def row_to_dict(self, row: pyodbc.Row) -> dict:
        """Convert a :class:`pyodbc.Row` to a plain dictionary.

        Args:
            row: A single row returned by pyodbc.

        Returns:
            A dict mapping column names to values, or an empty dict for ``None``.
        """
        if row is None:
            return {}
        columns = [column[0] for column in row.cursor_description]
        return dict(zip(columns, row))

    def rows_to_dicts(self, rows: List[pyodbc.Row]) -> List[dict]:
        """Convert a list of :class:`pyodbc.Row` objects to a list of dicts.

        Args:
            rows: Rows returned by pyodbc.

        Returns:
            A list of dictionaries, one per row.
        """
        return [self.row_to_dict(row) for row in rows]
