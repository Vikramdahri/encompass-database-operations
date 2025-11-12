# logging_config.py
import logging
import sys
from logging.handlers import RotatingFileHandler

def setup_logging(
    log_file: str,
    console_level: int = logging.INFO,      # ← No DEBUG spam on screen
    file_level: int = logging.DEBUG,        # ← Full debug in file
    max_bytes: int = 10 * 1024 * 1024,       # 10 MB
    backup_count: int = 5
) -> logging.Logger:
    """
    Root logger:
        * Console → INFO+ (clean, no per-row spam)
        * File    → DEBUG+ (full audit trail)
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()

    # ---------- CONSOLE (INFO+) ----------
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(console_level)
    console_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(message)s",
        datefmt="%H:%M:%S"
    )
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)

    # ---------- FILE (DEBUG+) ----------
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    file_handler.setLevel(file_level)
    file_fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(threadName)-12s | %(filename)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)

    return logger