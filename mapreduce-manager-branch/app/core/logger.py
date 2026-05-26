"""
Structured logging and context management module.

This module provides a unified logging interface designed for cloud-native
environments. It implements JSON-formatted output for machine readability
and leverages `contextvars` to maintain an asynchronous-safe 'job_id' context.

This architecture ensures that log correlation is maintained across the entire
distributed execution lifecycle, similar to Mapped Diagnostic Context (MDC)
patterns in Java/SLF4J.
"""

import logging
import sys
from contextvars import ContextVar

from pythonjsonlogger import json

#: Async-safe context variable to track Job IDs across concurrent tasks.
#: Defaults to 'system' for non-job related orchestration events.
job_id_var: ContextVar[str] = ContextVar("job_id", default="system")


class JobContextFilter(logging.Filter):
    """
    Logging filter for dynamic context injection.

    This filter intercepts every log record and injects the current value
    stored in :data:`job_id_var`. This enables a single request to be traced
    through multiple asynchronous operations.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Injects the current job_id into the log record.

        :param record: The log record to be modified.
        :type record: logging.LogRecord
        :return: Always returns True to ensure the record is processed.
        :rtype: bool
        """
        record.job_id = job_id_var.get()  # type: ignore[attr-defined]
        return True


def setup_logging() -> None:
    """
    Configures the root logger for structured JSON output to stdout.

    This setup clears default handlers (to avoid duplicate logs from Uvicorn)
    and establishes a JSON formatter compatible with cluster-level log
    aggregators like Loki, Fluentd, or the ELK stack.

    **Fields Included in Output:**
        * **asctime**: ISO8601 timestamp.
        * **levelname**: Standard log level (INFO, WARN, ERROR).
        * **name**: The name of the module generating the log.
        * **message**: The log message.
        * **job_id**: The correlation ID injected via JobContextFilter.

    :return: None
    :rtype: None
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent duplicate logging by clearing pre-existing framework handlers
    root_logger.handlers.clear()

    # Direct logs to stdout to allow Kubernetes to capture them as container logs
    handler = logging.StreamHandler(sys.stdout)

    # Structured format required for modern observability platforms.
    formatter = json.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(message)s %(job_id)s'
    )

    handler.setFormatter(formatter)

    # Attach the context filter to the primary output handler
    handler.addFilter(JobContextFilter())

    root_logger.addHandler(handler)