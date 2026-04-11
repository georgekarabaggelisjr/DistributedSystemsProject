import enum
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Text, ForeignKey, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
import uuid

Base = declarative_base()


# --- Enums ---

class JobStatus(str, enum.Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class JobFormat(str, enum.Enum):
    JSON = "JSON"
    AVRO = "AVRO"

# --- Tables ---

class JobRecord(Base):
    __tablename__ = "jobs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.SUBMITTED)
    format = Column(Enum(JobFormat), nullable=False)

    input_filename = Column(String(255), nullable=False)
    mapper_code_path = Column(Text, nullable=False)
    reducer_code_path = Column(Text, nullable=False)
    output_path = Column(Text, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now())

    # Progress Tracking
    total_chunks = Column(Integer, default=0, nullable=False)
    completed_map_chunks = Column(Integer, default=0, nullable=False)
    completed_reduce_chunks = Column(Integer, default=0, nullable=False)


class ManagerRecord(Base):
    __tablename__ = "managers"

    id = Column(String(255), primary_key=True)  # Manager Pod ID
    is_active = Column(Boolean, default=True)
    last_update = Column(DateTime(timezone=True), server_default=func.now())


class SystemConfigRecord(Base):
    __tablename__ = "system_config"

    config_key = Column(String(100), primary_key=True)
    config_value = Column(Text, nullable=False)
    description = Column(Text)