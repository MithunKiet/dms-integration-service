"""Logging configuration for the DMS Integration Service."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime


_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_BACKUP_COUNT = 30  # Keep 30 days of logs


class DailyDatedFileHandler(logging.Handler):
    """Write logs to a file named ``DDMMYYYY.log`` and switch file when day changes."""

    def __init__(self, log_dir: str, backup_count: int, encoding: str = "utf-8") -> None:
        super().__init__()
        self.log_dir = log_dir
        self.backup_count = backup_count
        self.encoding = encoding
        self._current_date: date | None = None
        self._file_handler: logging.FileHandler | None = None
        os.makedirs(self.log_dir, exist_ok=True)
        self._open_for_date(date.today())

    def _build_log_path(self, log_date: date) -> str:
        return os.path.join(self.log_dir, f"{log_date:%d%m%Y}.log")

    def _open_for_date(self, log_date: date) -> None:
        if self._file_handler is not None:
            self._file_handler.close()

        self._file_handler = logging.FileHandler(
            filename=self._build_log_path(log_date),
            encoding=self.encoding,
        )
        self._file_handler.setLevel(self.level)
        if self.formatter is not None:
            self._file_handler.setFormatter(self.formatter)

        self._current_date = log_date
        self._cleanup_old_logs()

    def _cleanup_old_logs(self) -> None:
        dated_logs: list[tuple[date, str]] = []

        for filename in os.listdir(self.log_dir):
            if not filename.lower().endswith(".log"):
                continue

            stem = os.path.splitext(filename)[0]
            try:
                file_date = datetime.strptime(stem, "%d%m%Y").date()
            except ValueError:
                continue

            dated_logs.append((file_date, filename))

        dated_logs.sort(key=lambda item: item[0], reverse=True)

        for _, filename in dated_logs[self.backup_count :]:
            try:
                os.remove(os.path.join(self.log_dir, filename))
            except OSError:
                # Best-effort cleanup; do not fail app startup because of old files.
                pass

    def setFormatter(self, fmt: logging.Formatter) -> None:
        super().setFormatter(fmt)
        if self._file_handler is not None:
            self._file_handler.setFormatter(fmt)

    def setLevel(self, level: int) -> None:
        super().setLevel(level)
        if self._file_handler is not None:
            self._file_handler.setLevel(level)

    def emit(self, record: logging.LogRecord) -> None:
        self.acquire()
        try:
            today = date.today()
            if today != self._current_date:
                self._open_for_date(today)

            if self._file_handler is not None:
                self._file_handler.emit(record)
        finally:
            self.release()

    def close(self) -> None:
        self.acquire()
        try:
            if self._file_handler is not None:
                self._file_handler.close()
                self._file_handler = None
            super().close()
        finally:
            self.release()


def setup_logging(log_level: str = "INFO", log_dir: str = "logs") -> None:
    """Configure root logger with a daily-dated file handler and a console handler.

    Args:
        log_level: Logging level string (e.g. ``"INFO"``, ``"DEBUG"``).
        log_dir: Directory where log files will be written.
    """
    os.makedirs(log_dir, exist_ok=True)

    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    formatter = logging.Formatter(fmt=_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # Active log file is named like 12052026.log and switches automatically at midnight.
    file_handler = DailyDatedFileHandler(
        log_dir=log_dir,
        backup_count=_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # Avoid duplicate handlers when called more than once.
    root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
