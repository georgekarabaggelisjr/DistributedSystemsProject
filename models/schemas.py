import enum
from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID
from datetime import datetime
from typing import Optional


# --- Μεταφορά των Enums από το dds_models που διαγράφηκε ---
class JobStatus(str, enum.Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"


class JobFormat(str, enum.Enum):
    JSON = "JSON"
    AVRO = "AVRO"
    TEXT = "TEXT"
    CSV = "CSV"


# --- Job Schemas ---

class JobResponse(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )

    id: UUID = Field(..., validation_alias="jobId")
    status: JobStatus
    completed_map_chunks: int = Field(..., validation_alias="completedMapChunks")
    completed_reduce_chunks: int = Field(..., validation_alias="completedReduceChunks")
    created_at: datetime = Field(..., validation_alias="createdAt")
    updated_at: datetime = Field(..., validation_alias="updatedAt")


# --- Admin & Config Schemas ---

class UserCreateSchema(BaseModel):
    username: str
    email: str
    password: str


class ConfigUpdateSchema(BaseModel):
    # Αλλαγή των ονομάτων σε key/value για να κάνει match με το config.key του admin.py
    key: str = Field(..., examples=["max_parallel_workers"])
    value: str
    description: Optional[str] = None


class ManagerStatusSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    is_active: bool
    last_update: datetime


# --- Generic Schemas ---

class MessageResponse(BaseModel):
    message: str
    job_id: Optional[UUID] = None