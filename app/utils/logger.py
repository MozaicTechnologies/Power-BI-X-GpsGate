import logging
import coloredlogs
import os
import sys
import re
from contextlib import contextmanager
from contextvars import ContextVar
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
_task_file_handler: ContextVar[logging.FileHandler | None] = ContextVar(
    "task_file_handler", default=None
)


def _get_numeric_level(level: str | None = None) -> int:
    name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    return getattr(logging, name, logging.INFO)


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
    return _shared_file_handler


def setup_logger(name: str, level: str = None) -> logging.Logger:
    """Return a named logger with file + colored-console output.

    Safe to call multiple times — handlers are added only once per logger.
    Set LOG_LEVEL env var to DEBUG/INFO/WARNING/ERROR to control verbosity.
    """
    logger = logging.getLogger(name)
    task_handler = _task_file_handler.get()
    if logger.handlers:
        if task_handler is not None and task_handler not in logger.handlers:
            logger.addHandler(task_handler)
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


@contextmanager
def task_log_context(task_type: str, task_id: str):
    """Copy logs emitted during one Celery task to a dedicated file."""
    safe_type = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_type))
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(task_id))
    task_dir = os.path.join("logs", safe_type)
    os.makedirs(task_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(task_dir, f"{safe_type}_{timestamp}_{safe_id}.log")

    handler = logging.FileHandler(path, encoding="utf-8")
    # INFO is intentional: DEBUG payloads may contain customer tokens.
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(_FMT))
    token = _task_file_handler.set(handler)

    attached = []
    for value in logging.Logger.manager.loggerDict.values():
        # Propagating loggers are captured by root; attaching to both would
        # duplicate every line in the dedicated file.
        if isinstance(value, logging.Logger) and not value.propagate and handler not in value.handlers:
            value.addHandler(handler)
            attached.append(value)
    root = logging.getLogger()
    if handler not in root.handlers:
        root.addHandler(handler)
        attached.append(root)

    try:
        yield os.path.abspath(path)
    finally:
        _task_file_handler.reset(token)
        for target in attached:
            target.removeHandler(handler)
        handler.close()
