"""
Data Transfer Objects (DTOs) for API and message broker communication.

This module defines the Pydantic models used to validate incoming HTTP requests
and serialize outgoing AMQP message payloads. These models serve as the
formal interface contract between the Python orchestrator and the Java
worker nodes, ensuring data integrity, type safety, and security across the
distributed system boundary.

The models utilize Pydantic v2 features, including alias generators to
seamlessly bridge Python's `snake_case` and Java's `camelCase` naming conventions.
"""

import uuid
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from pydantic.alias_generators import to_camel
from app.models.enums import TaskType, JobStatus


class ScheduleJobRequest(BaseModel):
    """
    Payload for initiating a new MapReduce execution via HTTP.

    This model acts as the primary validation gate for tenant job submissions.
    It encapsulates all necessary execution parameters, including object storage
    references, bytecode identifiers, and explicit topology overrides.
    """
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    job_id: uuid.UUID = Field(
        description="The primary universally unique identifier (UUID) for the job execution."
    )

    # --- SECURITY UPDATE ---
    # user_id is intentionally omitted from the client-facing HTTP payload.
    # Tenant isolation and sandboxing are now strictly enforced by extracting
    # the user ID from the cryptographically verified JWT Bearer token.

    format: str = Field(
        description="The data serialization format of the input file (e.g., 'JSON', 'TEXT', 'AVRO')."
    )
    input_filename: str = Field(
        description="The specific object key of the source dataset within the tenant's bucket."
    )
    file_size: int = Field(
        gt=0,
        description="Total size of the input file in bytes. Used to calculate logical data splits."
    )
    mapper_filename: str = Field(
        description="The filename of the compiled Mapper bytecode (e.g., 'WordCountMapper.class')."
    )
    reducer_filename: str = Field(
        description="The filename of the compiled Reducer bytecode (e.g., 'WordCountReducer.class')."
    )
    num_reducers: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional manual override for the number of reduce partitions. Bypasses auto-scaling."
    )
    num_mappers: Optional[int] = Field(
        default=None,
        ge=1,
        description="Optional manual override for the number of concurrent Map worker pods to provision."
    )


class TaskMessage(BaseModel):
    """
    AMQP message payload dispatched to RabbitMQ worker queues.

    This model represents a single unit of work (a 'chunk') sent to a Java worker.
    It is serialized to JSON with camelCase keys to match the native field naming
    of Java POJOs used in the Worker's TaskPayload class.
    """
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True
    )

    task_id: str = Field(description="Unique task identifier (e.g., 'map-chunk-0').")
    job_id: str = Field(description="The parent job identifier for context tracking.")
    job_token: str = Field(
        description="Secure cryptographic token used by the Worker to authorize gRPC shuffle transfers."
    )
    task_type: TaskType = Field(description="Routing hint for the worker (MAP or REDUCE).")

    # Infrastructure target metadata
    bucket_name: str = Field(description="S3 bucket containing the raw data (e.g., 'data' or 'results').")
    object_name: str = Field(description="S3 key for the raw data object.")

    # Chunking boundaries
    byte_offset: int = Field(description="Starting position for file-pointer seeks.")
    byte_length: int = Field(description="Total bytes to read from the offset.")
    num_reducers: int = Field(description="Used to determine partition assignment during shuffles.")

    # User bytecode metadata for dynamic execution
    user_code_bucket: str = Field(description="S3 bucket containing user-provided .class files.")
    user_code_object: str = Field(description="S3 key for the user-provided bytecode.")
    class_name: str = Field(description="The fully qualified Java class for reflection-based instantiation.")

    # Peer-to-Peer Shuffle routing table
    worker_endpoints: List[str] = Field(
        default_factory=list,
        description="List of address@@IP:Port endpoints for worker-to-worker data transfers."
    )


class WorkerSignal(BaseModel):
    """
    Standardized telemetry and lifecycle payload emitted by ephemeral Worker nodes.

    Consumed by the Manager's RabbitMQ consumer loop to reconcile distributed state,
    handle reactive lineage recovery, and track the active cluster topology.
    """
    model_config = ConfigDict(populate_by_name=True)

    job_id: str = Field(
        alias="jobId",
        description="The UUID of the parent MapReduce job."
    )
    task_id: str = Field(
        alias="taskId",
        description="The deterministic identifier of the completed chunk or partition."
    )
    job_token: Optional[str] = Field(
        default=None,
        alias="jobToken",
        description="Secure cryptographic token echoed back by the worker to authenticate the signal."
    )
    status: str = Field(
        alias="state",
        description="The reported execution state (e.g., 'COMPLETED', 'FAILED', or 'IN_PROGRESS')."
    )
    error: Optional[str] = Field(
        default=None,
        description="Diagnostic stack trace provided if the worker encountered an exception."
    )

    # Peer-to-Peer Shuffle worker discovery
    worker_bind_address: Optional[str] = Field(
        default=None,
        alias="workerBindAddress",
        description="The network address (node@@IP:Port) where the ESS is serving partitioned data."
    )


class JobStatusResponse(BaseModel):
    """
    Outgoing HTTP response payload representing real-time job progress.

    This Data Transfer Object (DTO) forms the strict API contract with the
    internal UI Service. It guarantees that internal database types (such as
    PostgreSQL UUIDs and Native Datetimes) are safely and consistently serialized
    into standard JSON primitives (strings and ISO 8601 timestamps) before
    crossing the network boundary.
    """
    model_config = ConfigDict(populate_by_name=True)

    id: uuid.UUID = Field(
        description="The primary universally unique identifier (UUID) of the job."
    )
    status: JobStatus = Field(
        description="The current execution state (e.g., SUBMITTED, RUNNING, COMPLETED)."
    )
    completed_map_chunks: int = Field(
        description="The total count of successfully processed Map tasks."
    )
    completed_reduce_chunks: int = Field(
        description="The total count of successfully processed Reduce tasks."
    )
    created_at: Optional[datetime] = Field(
        default=None,
        description="The exact UTC timestamp when the job was initially submitted."
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        description="The exact UTC timestamp of the last recorded state mutation or heartbeat."
    )