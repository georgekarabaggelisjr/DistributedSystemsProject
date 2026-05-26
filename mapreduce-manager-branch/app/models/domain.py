"""
Domain models representing internal business entities.

This module defines the core `Job` entity used throughout the application's
internal layers. These models serve as the "Source of Truth" for job state,
mapping directly to the PostgreSQL schema while providing type-safe structures
for the repository and orchestration services.

The models are powered by Pydantic to ensure strict validation during
data exchange between the API, the database, and the message broker.
"""

import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
from app.models.enums import JobStatus, JobFormat


class Job(BaseModel):
    """
    Representation of a MapReduce Job entity.

    This model tracks the entire lifecycle of a distributed job, from
    initial submission to partition completion and final state transition.
    It is the primary object used by the Orchestrator to determine phase
    transitions (Map-to-Reduce).

    Note: Execution class names are dynamically derived at runtime from the
    provided bytecode filenames, reducing data redundancy and payload size.

    Attributes:
        id (uuid.UUID): The primary identifier for the job.
        user_id (uuid.UUID): The identifier of the user who owns the job,
            used to resolve sandboxed S3 storage paths.
        status (JobStatus): The current execution state (e.g., SUBMITTED,
            RUNNING, COMPLETED).
        format (JobFormat): The encoding format of the input data
            (e.g., JSON, TEXT).
        input_filename (str): The name of the source data file located within
            the user's sandboxed 'data' bucket directory.
        mapper_filename (str): The filename of the compiled Java Mapper
            bytecode residing in the user's 'code' directory.
        reducer_filename (str): The filename of the compiled Java Reducer
            bytecode residing in the user's 'code' directory.
        num_reducers (int): The target number of reduce partitions configured
            for this specific job execution.
        num_mappers (Optional[int]): An optional user override for the number
            of concurrent Map worker pods to spawn.
        total_chunks (int): The total number of logical data fragments
            calculated for the input file.
        completed_map_chunks (int): The count of successfully processed
            and acknowledged Map tasks.
        completed_reduce_chunks (int): The count of successfully
            processed and acknowledged Reduce tasks.
        created_at (Optional[datetime]): Timestamp of job record insertion.
        updated_at (Optional[datetime]): Timestamp of the last record mutation.
    """

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "id": "11111111-1111-1111-1111-111111111111",
                "user_id": "22222222-2222-2222-2222-222222222222",
                "status": "SUBMITTED",
                "format": "TEXT",
                "input_filename": "logs.txt",
                "mapper_filename": "WordCountMapper.class",
                "reducer_filename": "WordCountReducer.class",
                "num_mappers": 15,
                "num_reducers": 3,
                "total_chunks": 12
            }
        }
    )

    # --- Core Identification ---
    # Used for correlation across K8s, RabbitMQ, and S3 paths
    id: uuid.UUID
    user_id: uuid.UUID

    # --- Configuration and Metadata ---
    status: JobStatus = Field(default=JobStatus.SUBMITTED)
    format: JobFormat

    # --- Multi-Tenant File Identifiers ---
    # Explicit class names have been removed. The Orchestrator derives the
    # execution classes directly from these filenames (e.g., stripping '.class').
    input_filename: str
    mapper_filename: str
    reducer_filename: str

    # --- Dynamic Scaling ---
    # Persists the partition count and pod overrides across the async boundaries
    # of the orchestration pipeline.
    num_reducers: int = Field(default=3)
    num_mappers: Optional[int] = Field(default=None)

    # --- Orchestration Progress Tracking ---
    # Atomic counters updated by the TaskEventProcessor to manage phase barriers
    total_chunks: int = Field(default=0)
    completed_map_chunks: int = Field(default=0)
    completed_reduce_chunks: int = Field(default=0)

    # --- Audit Timestamps ---
    # Managed automatically by the database layer (PostgreSQL NOW())
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None