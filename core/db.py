"""Thread-safe database connection manager for HMIS, DMS, and INTEGRATION databases."""

from __future__ import annotations

import logging
import threading
from typing import Any, List, Optional, Sequence, Tuple

try:
    import pyodbc
except ImportError:  # pragma: no cover
    pyodbc = None  # type: ignore[assignment]

from core.exceptions import DatabaseConnectionError
from models.enums import DbType

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages pyodbc connections to the three application databases.

    Connections are created lazily on first access and reused across calls.
    A :class:`threading.Lock` guards each connection so that concurrent threads
    do not corrupt shared state.

    Usage::

        db = DatabaseManager(hmis_cs, dms_cs, integration_cs)
        conn = db.get_hmis_connection()
        rows = db.execute_query(conn, "SELECT 1")
        db.close_all()
    """

    def __init__(
        self,
        hmis_connection_string: str,
        dms_connection_string: str,
        integration_connection_string: str,
    ) -> None:
        self._hmis_cs = hmis_connection_string
        self._dms_cs = dms_connection_string
        self._integration_cs = integration_connection_string

        self._hmis_conn: Optional[Any] = None
        self._dms_conn: Optional[Any] = None
        self._integration_conn: Optional[Any] = None

        self._hmis_lock = threading.Lock()
        self._dms_lock = threading.Lock()
        self._integration_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public connection accessors
    # ------------------------------------------------------------------

    def get_hmis_connection(self) -> Any:
        """Return (or lazily create) the HMIS database connection.

        Returns:
            An open :class:`pyodbc.Connection` with ``autocommit=False``.

        Raises:
            :class:`~core.exceptions.DatabaseConnectionError`: If the connection
                cannot be established.
        """
        with self._hmis_lock:
            if self._hmis_conn is None:
                self._hmis_conn = self._connect(self._hmis_cs, DbType.HMIS)
            return self._hmis_conn

    def get_dms_connection(self) -> Any:
        """Return (or lazily create) the DMS database connection.

        Returns:
            An open :class:`pyodbc.Connection` with ``autocommit=False``.

        Raises:
            :class:`~core.exceptions.DatabaseConnectionError`: If the connection
                cannot be established.
        """
        with self._dms_lock:
            if self._dms_conn is None:
                self._dms_conn = self._connect(self._dms_cs, DbType.DMS)
            return self._dms_conn

    def get_integration_connection(self) -> Any:
        """Return (or lazily create) the INTEGRATION database connection.

        Returns:
            An open :class:`pyodbc.Connection` with ``autocommit=False``.

        Raises:
            :class:`~core.exceptions.DatabaseConnectionError`: If the connection
                cannot be established.
        """
        with self._integration_lock:
            if self._integration_conn is None:
                self._integration_conn = self._connect(
                    self._integration_cs, DbType.INTEGRATION
                )
            return self._integration_conn

    # ------------------------------------------------------------------
    # Health / test helpers
    # ------------------------------------------------------------------

    def test_connection(self, db_type: DbType) -> bool:
        """Verify that a connection to *db_type* can be established.

        A simple ``SELECT 1`` query is used as a liveness probe.  The method
        does not cache the test connection; it is opened and closed immediately.

        Args:
            db_type: Which database to probe.

        Returns:
            ``True`` if the test succeeds, ``False`` otherwise.
        """
        cs_map = {
            DbType.HMIS: self._hmis_cs,
            DbType.DMS: self._dms_cs,
            DbType.INTEGRATION: self._integration_cs,
        }
        connection_string = cs_map[db_type]
        try:
            conn = self._connect(connection_string, db_type)
            self.execute_query(conn, "SELECT 1")
            conn.close()
            return True
        except Exception as exc:
            logger.warning("test_connection failed for %s: %s", db_type.value, exc)
            return False

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def execute_query(
        self,
        conn: Any,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> List[Any]:
        """Execute a SELECT statement and return all rows.

        Args:
            conn: An active :class:`pyodbc.Connection`.
            sql: The SQL query string (use ``?`` placeholders for parameters).
            params: Optional sequence of parameter values.

        Returns:
            A list of :class:`pyodbc.Row` objects.
        """
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()

    def execute_non_query(
        self,
        conn: Any,
        sql: str,
        params: Optional[Sequence[Any]] = None,
    ) -> int:
        """Execute an INSERT / UPDATE / DELETE statement.

        The caller is responsible for committing or rolling back the transaction.

        Args:
            conn: An active :class:`pyodbc.Connection`.
            sql: The SQL statement (use ``?`` placeholders for parameters).
            params: Optional sequence of parameter values.

        Returns:
            Number of rows affected.
        """
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.rowcount
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close_all(self) -> None:
        """Close all open database connections gracefully.

        Errors during individual closes are logged but do not propagate so
        that all connections are attempted.
        """
        for attr, lock, label in (
            ("_hmis_conn", self._hmis_lock, "HMIS"),
            ("_dms_conn", self._dms_lock, "DMS"),
            ("_integration_conn", self._integration_lock, "INTEGRATION"),
        ):
            with lock:
                conn = getattr(self, attr)
                if conn is not None:
                    try:
                        conn.close()
                        logger.debug("Closed %s database connection.", label)
                    except Exception as exc:
                        logger.warning(
                            "Error closing %s connection: %s", label, exc
                        )
                    finally:
                        setattr(self, attr, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self, connection_string: str, db_type: DbType) -> Any:
        """Open a new pyodbc connection for the given connection string.

        Args:
            connection_string: ODBC connection string.
            db_type: Used only for logging / error messages.

        Returns:
            An open :class:`pyodbc.Connection` with ``autocommit=False``.

        Raises:
            :class:`~core.exceptions.DatabaseConnectionError`: On failure.
        """
        if pyodbc is None:
            raise DatabaseConnectionError(
                "pyodbc is not installed; cannot connect to database."
            )
        try:
            conn = pyodbc.connect(connection_string, autocommit=False)
            logger.debug("Opened %s database connection.", db_type.value)
            return conn
        except Exception as exc:
            raise DatabaseConnectionError(
                f"Failed to connect to {db_type.value} database: {exc}"
            ) from exc
