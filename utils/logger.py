import logging
import coloredlogs
import os
from datetime import datetime
import sys

# Create logs directory if it doesn't exist
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)

# Configure file handler with UTF-8 encoding
log_file = os.path.join(logs_dir, f'bot_{datetime.now().strftime("%Y%m%d")}.log')
file_handler = logging.FileHandler(log_file, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_format)

# Configure console handler with UTF-8 encoding
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_format = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
console_handler.setFormatter(console_format)


def setup_logger(name: str, level: str = None) -> logging.Logger:
    """
    Set up a logger with both console and file output

    Args:
        name: Logger name (usually __name__ of the module)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) - defaults to env LOG_LEVEL or INFO

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Prevent adding handlers multiple times
    if not logger.handlers:
        # Get log level from parameter or environment
        if level is None:
            level = os.getenv('LOG_LEVEL', 'INFO').upper()

        numeric_level = getattr(logging, level, logging.INFO)

        # Set the logger level
        logger.setLevel(numeric_level)

        # Set handler levels to match
        file_handler.setLevel(numeric_level)
        console_handler.setLevel(numeric_level)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        # Configure coloredlogs with UTF-8 encoding
        coloredlogs.install(
            level=numeric_level,
            logger=logger,
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level_styles={
                "debug": {"color": "blue"},
                "info": {"color": "green"},
                "warning": {"color": "yellow", "bold": True},
                "error": {"color": "red", "bold": True},
                "critical": {"color": "red", "bold": True, "background": "white"},
            },
            field_styles={
                "asctime": {"color": "cyan"},
                "name": {"color": "magenta"},
                "levelname": {"color": "white", "bold": True},
            },
        )

    return logger
