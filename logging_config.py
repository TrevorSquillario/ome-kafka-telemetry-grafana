"""Centralized logging configuration for the application.
"""
import logging
from typing import Optional


def configure_logging(level: Optional[int] = None) -> None:
    """Configure root logger with a simple standardized formatter.

    Args:
        level: Root logging level (int or None). If None, preserves existing
               level or defaults to INFO.
    """
    root = logging.getLogger()

    # Remove existing handlers to avoid duplicate logs
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler()
    fmt = "%(asctime)s: %(module)s.%(funcName)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if level is not None:
        root.setLevel(level)


__all__ = ["configure_logging"]
