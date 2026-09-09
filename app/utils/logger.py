import logging
import coloredlogs
import os
import sys
import re
from datetime import datetime

_FMT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

_COLOR_LEVEL_STYLES = {
    "debug":    {"color": "blue"},
    "info":     {"color": "green"},
    "warning":  {"color": "yellow", "bold": True},
    "error":    {"color": "red", "bold": True},
    "critical": {"color": "red", "bold": True, "background": "white"},
}
_COLOR_FIELD_STYLES = {
    "asctime":   {"color": "cyan"},
    "name":      {"color": "magenta"},
    "levelname": {"color": "white", "bold": True},
}

_shared_file_handler: logging.FileHandler | None = None
_dedicated_file_handlers: dict[str, logging.FileHandler] = {}
LOG_FILES_TO_KEEP = max(1, int(os.getenv("LOG_FILES_TO_KEEP", "2")))


def _get_numeric_level(level: str | None = None) -> int:
    name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    return getattr(logging, name, logging.INFO)


def _cleanup_old_log_files(logs_dir: str, filename_prefix: str, current_file: str) -> None:
    """Keep only the newest configured number of files for one log prefix."""
    current_path = os.path.abspath(current_file)
    try:
        candidates = [
            os.path.abspath(os.path.join(logs_dir, entry.name))
            for entry in os.scandir(logs_dir)
            if entry.is_file()
            and entry.name.startswith(f"{filename_prefix}_")
            and entry.name.endswith(".log")
        ]
        candidates.sort(
            key=lambda path: (os.path.getmtime(path), path),
            reverse=True,
        )

        keep = {current_path}
        for path in candidates:
            if len(keep) >= LOG_FILES_TO_KEEP:
                break
            keep.add(path)

        for path in candidates:
            if path not in keep:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    # Another web/worker process may have cleaned it first.
                    pass
    except OSError:
        # Logging initialization must never stop the application from starting.
        logging.getLogger(__name__).exception(
            "Failed to clean old log files for prefix=%s", filename_prefix
        )


def _get_file_handler() -> logging.FileHandler:
    global _shared_file_handler
    if _shared_file_handler is None:
        logs_dir = "logs"
        os.makedirs(logs_dir, exist_ok=True)
        log_file = os.path.join(logs_dir, f"app_{datetime.now().strftime('%Y%m%d')}.log")
        h = logging.FileHandler(log_file, encoding="utf-8")
        h.setLevel(_get_numeric_level())
        h.setFormatter(logging.Formatter(_FMT))
        _shared_file_handler = h
        _cleanup_old_log_files(logs_dir, "app", log_file)
    return _shared_file_handler


def setup_logger(name: str, level: str = None) -> logging.Logger:
    """Return a named logger with file + colored-console output.

    Safe to call multiple times — handlers are added only once per logger.
    Set LOG_LEVEL env var to DEBUG/INFO/WARNING/ERROR to control verbosity.
    """
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    numeric_level = _get_numeric_level(level)
    logger.setLevel(numeric_level)
    logger.propagate = False  # prevent double-printing through root logger

    # All loggers share one daily file
    logger.addHandler(_get_file_handler())

    # Colored console output
    coloredlogs.install(
        level=numeric_level,
        logger=logger,
        fmt=_FMT,
        level_styles=_COLOR_LEVEL_STYLES,
        field_styles=_COLOR_FIELD_STYLES,
        stream=sys.stdout,
    )

    return logger


def setup_dedicated_file_logger(
    name: str,
    filename_prefix: str,
    level: str = "INFO",
) -> logging.Logger:
    """Return a logger that writes only to its own daily diagnostic file.

    The handler is shared inside the process, so importing this logger from
    multiple pipeline modules does not duplicate lines. It intentionally has
    no console or shared app-log handler.
    """
    safe_prefix = re.sub(r"[^A-Za-z0-9_.-]+", "_", filename_prefix)
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    path = os.path.abspath(
        os.path.join(logs_dir, f"{safe_prefix}_{datetime.now().strftime('%Y%m%d')}.log")
    )

    handler = _dedicated_file_handlers.get(path)
    numeric_level = _get_numeric_level(level)
    if handler is None:
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setLevel(numeric_level)
        handler.setFormatter(logging.Formatter(_FMT))
        _dedicated_file_handlers[path] = handler
        _cleanup_old_log_files(logs_dir, safe_prefix, path)

    logger = logging.getLogger(name)
    logger.setLevel(numeric_level)
    logger.propagate = False
    if handler not in logger.handlers:
        logger.addHandler(handler)
    return logger
