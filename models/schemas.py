from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID
from datetime import datetime
from typing import Optional, List
from models.dds_models import JobStatus, JobFormat

# --- Job Schemas ---

class JobBase(BaseModel):
    format: JobFormat
    input_filename: str

class JobResponse(JobBase):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    total_chunks: int
    completed_map_chunks: int
    completed_reduce_chunks: int
    output_path: str

# --- Admin & Config Schemas ---

class UserCreateSchema(BaseModel):
    username: str
    email: str
    password: str

class ConfigUpdateSchema(BaseModel):

    config_key: str = Field(..., example="max_parallel_workers")
    config_value: str
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