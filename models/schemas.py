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
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    status: JobStatus
    completed_map_chunks: int
    completed_reduce_chunks: int
    created_at: datetime
    updated_at: datetime

    # Αυτά τα πεδία γίνονται Optional με default None, επειδή ο Manager
    # δεν τα επιστρέφει στο status payload του βάσει του API Contract.
    user_id: Optional[UUID] = None
    format: Optional[JobFormat] = None
    input_filename: Optional[str] = None
    total_chunks: Optional[int] = None
    output_path: Optional[str] = None


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