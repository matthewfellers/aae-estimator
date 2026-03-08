"""
Local Sync Logger — file-based logging for the polling service.

Writes to qb_sync.log on the local machine for debugging/monitoring,
independent of the Supabase sync log table.
"""

import logging
import os
from datetime import datetime


def setup_logging(log_file: str = "qb_sync.log", level: str = "INFO"):
    """
    Configure logging for the polling service.

    Logs to both file and console. The file log persists for troubleshooting.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    # Create logger
    logger = logging.getLogger("qb_poller")
    logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates on re-init
    logger.handlers.clear()

    # File handler — append mode, UTF-8
    log_dir = os.path.dirname(os.path.abspath(log_file))
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized — level={level}, file={log_file}")
    return logger
