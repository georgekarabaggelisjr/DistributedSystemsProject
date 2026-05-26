"""
Core enumerations for the MapReduce domain.

This module defines the static states, formats, and types used across the
application for database tracking, worker node lifecycle management,
and distributed task execution.

Design Rationale:
    Using string-based enums (`str, Enum`) ensures seamless compatibility
    with JSON serialization for RabbitMQ message payloads and direct
    string-mapping in PostgreSQL/SQLAlchemy. It avoids the maintenance
    overhead of integer-to-state mapping.
"""

from enum import Enum


class JobFormat(str, Enum):
    """
    Supported input data formats for MapReduce jobs.

    This enumeration defines the serialization type of the source data
    residing in S3-compatible storage (MinIO). Workers use this value
    to instantiate the correct data parser.

    Attributes:
        JSON: Text-based, structured format widely used for semi-structured data documents.
        AVRO: Binary serialization format for high-throughput data.
        XML: Hierarchical markup language used for structured document exchange.
        TEXT: Standard structured text format, typically parsed line-by-line for raw string inputs.
        CSV: Comma-Separated Values format representing tabular data records, typically parsed line-by-line using a delimiter.
    """
    JSON = 'JSON'
    AVRO = 'AVRO'
    XML = 'XML'
    TEXT = 'TEXT'
    CSV = 'CSV'


class JobStatus(str, Enum):
    """
    Lifecycle states of a distributed MapReduce job.

    Tracks the high-level progression of a job as it moves through the
    orchestration pipeline managed by the FastAPI server. This is the
    primary state used for user-facing status reporting.

    Attributes:
        SUBMITTED: Job record created; metadata resolved, awaiting execution.
        RUNNING: Infrastructure is provisioned; tasks are actively processing.
        COMPLETED: Successful finalization of all Map and Reduce phases.
        FAILED: Terminal state; execution halted due to unrecoverable errors.
    """
    SUBMITTED = 'SUBMITTED'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class TaskStatus(str, Enum):
    """
    Granular lifecycle states of individual Map or Reduce tasks.

    Used by the internal task tracking system (TaskEventProcessor) to
    monitor the progress of specific data fragments (chunks) as they
    are processed by worker nodes.

    Attributes:
        PENDING: Task exists in the queue but has not been claimed.
        IN_PROGRESS: A worker has successfully consumed the task message.
        FINISHED: Computation is complete and results are persisted to S3.
        FAILED: Task failed; may be subject to retry logic.
    """
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    FINISHED = 'FINISHED'
    FAILED = 'FAILED'


class TaskType(str, Enum):
    """
    Execution phase identifiers for worker routing.

    Determines the logic the worker should apply to the input data.
    RabbitMQ consumers use this to route tasks to the correct executor.

    Attributes:
        MAP: Initial transformation phase; produces intermediate key-value data.
        REDUCE: Final aggregation phase; merges intermediate data into results.
    """
    MAP = 'MAP'
    REDUCE = 'REDUCE'


class WorkerStatus(str, Enum):
    """
    Health and utilization states of Kubernetes worker pods.

    Reflects the current operational readiness of the containerized
    workers as monitored by the Kubernetes provider.

    Attributes:
        IDLE: Worker is healthy and listening for new task assignments.
        BUSY: Worker is actively performing computation.
        DOWN: Worker is unreachable, crashed, or terminated by the Manager.
    """
    IDLE = 'IDLE'
    BUSY = 'BUSY'
    DOWN = 'DOWN'