from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from typing import List, Optional
from api.deps import get_current_user, get_db
from models.schemas import JobStatusResponse
from services.orchestrator import JobOrchestrator

router = APIRouter()


@router.post("/", status_code=202)
async def submit_job(
        data_file: UploadFile = File(...),
        code_file: UploadFile = File(...),
        current_user: dict = Depends(get_current_user),
        db=Depends(get_db)
):
    """
    REST Endpoint for CLI: 'jobs submit <data> <code>'.

    1. Extracts binary streams from the Multipart/Form-data request.
    2. Passes the user identity and files to the JobOrchestrator.
    3. Returns the Job ID immediately (Asynchronous pattern).
    """
    pass


@router.get("/status", response_model=List[JobStatusResponse])
@router.get("/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(
        job_id: Optional[str] = None,
        current_user: dict = Depends(get_current_user),
        db=Depends(get_db)
):
    """
    REST Endpoint for CLI: 'jobs status' OR 'jobs status <id>'.

    1. Checks if the user is asking for a specific job or their full list.
    2. Verifies Admin roles via the JWT to decide if filtering by user_id is needed.
    3. Queries the DDS (PostgreSQL) via the Orchestrator.
    """
    pass


@router.get("/{job_id}/result")
async def get_job_result(
        job_id: str,
        current_user: dict = Depends(get_current_user),
        db=Depends(get_db)
):
    """
    REST Endpoint for CLI: 'jobs result <id>'.

    1. Validates that the job status is 'COMPLETED'.
    2. Validates ownership (User A cannot download User B's result unless Admin).
    3. Returns a StreamingResponse from MinIO to avoid buffering large files in UI memory.
    """
    pass