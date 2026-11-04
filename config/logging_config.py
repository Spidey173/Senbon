"""
Production-Grade Logging Configuration
======================================
Implements structured logging with JSON formatting support for Datadog/ELK/CloudWatch,
console colorization, file rotation, and contextual execution metadata.
"""

import os
import sys
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    """
    Formats log records as newline-delimited JSON objects for cloud log aggregators.
    Standard pattern used in Netflix and Uber distributed pipelines.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
            "process_id": record.process,
            "thread_name": record.threadName,
        }

        # Include custom context attributes if injected
        if hasattr(record, "batch_id"):
            log_payload["batch_id"] = record.batch_id
        if hasattr(record, "dataset"):
            log_payload["dataset"] = record.dataset
        if record.exc_info:
            log_payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_payload)


class ConsoleFormatter(logging.Formatter):
    """Clean, human-readable terminal formatter with color highlights."""

    COLORS = {
        logging.DEBUG: "\033[36m",    # Cyan
        logging.INFO: "\033[32m",     # Green
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",    # Red
        logging.CRITICAL: "\033[35m", # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelno, self.RESET)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = super().format(record)
        return f"{color}[{timestamp}] [{record.levelname:<8}] [{record.name}]{self.RESET} {msg}"


def setup_logging(
    log_level: str = "INFO",
    logs_dir: str = "logs",
    log_file_name: str = "pipeline.log",
    enable_json_logs: bool = False,
) -> None:
    """
    Initializes global logging with both console and rotating file handlers.
    """
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    root_logger.handlers = []

    # Console Stream Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    if enable_json_logs:
        console_handler.setFormatter(JsonFormatter())
    else:
        console_handler.setFormatter(ConsoleFormatter("%(message)s"))
    root_logger.addHandler(console_handler)

    # Rotating File Handler (Max 10MB per file, keeping last 5 backups)
    log_file_path = os.path.join(logs_dir, log_file_name)
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(numeric_level)
    file_handler.setFormatter(JsonFormatter())
    root_logger.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """
    Convenience factory to get a named logger.
    """
    return logging.getLogger(name)


# Initialize default root logging
setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"),
    logs_dir=os.getenv("LOGS_DIR", "logs"),
    enable_json_logs=os.getenv("LOG_FORMAT", "text").lower() == "json",
)

