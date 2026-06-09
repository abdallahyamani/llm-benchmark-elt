"""Logging configuration for the LLM Benchmark pipeline."""


import logging
import os


def configure_logging(level: str = "INFO") -> None:
    """Configure root logger with ISO timestamp format."""
    effective_level = os.environ.get("LOG_LEVEL", level).upper()

    logging.basicConfig(
        level=getattr(logging, effective_level, logging.INFO),
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
