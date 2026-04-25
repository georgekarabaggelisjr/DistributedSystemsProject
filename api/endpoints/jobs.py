from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import List, Optional, Union
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_current_user, get_db, get_orchestrator
from models.schemas import JobResponse
from services.manager_router import ManagerRouter
from services.orchestrator import JobOrchestrator
from services.storage_client import StorageClient

router = APIRouter()


@router.post("/", status_code=202)
async def submit_job(
        data_file: UploadFile = File(...),
        mapper_file: UploadFile = File(...),
        reducer_file: UploadFile = File(...),
        current_user: dict = Depends(get_current_user),
        orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    """
    REST Endpoint for CLI: 'jobs submit <data> <code>'.

    1. Extracts the user's id and the  binary streams from the Multipart/Form-data request.
    2. Passes the user identity and files to the JobOrchestrator.
    3. Returns the Job ID immediately (Asynchronous pattern).
    """
    # 1
    user_id = current_user["sub"]

    data_bytes = await data_file.read()
    mapper_bytes = await mapper_file.read()
    reducer_bytes = await reducer_file.read()

    # 2
    job_id = await orchestrator.submit_job(
        user_id=user_id,
        data_filename=data_file.filename,
        data_bytes=data_bytes,
        mapper_filename=mapper_file.filename,
        mapper_bytes=mapper_bytes,
        reducer_filename=reducer_file.filename,
        reducer_bytes=reducer_bytes
    )

    # 3
    return {
        "job_id": job_id,
        "message": "Job submitted successfully and is pending execution."
    }

@router.get("/", response_model=List[JobResponse])
async def list_user_jobs(
    current_user: dict = Depends(get_current_user),
    orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    """
    REST Endpoint for CLI: 'jobs status'.

    Getting the list of jobs and their status.

    There are 2 cases:
        1) Run by Admin: He has access to the status of all the jobs in the system.
        2) Run by User: He has access only to the status of his own jobs in the system.
    """
    user_id = current_user["sub"]
    roles = current_user.get("realm_access", {}).get("roles", [])
    is_admin = "admin" in roles

    if is_admin:
        return await orchestrator.get_all_jobs()
    return await orchestrator.get_user_jobs(user_id=user_id)

@router.get("/{job_id}/status", response_model=JobResponse)
async def get_single_job_status(
    job_id: str,
    current_user: dict = Depends(get_current_user),
    orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    """
        REST Endpoint for CLI: 'jobs status <job_id>'.

        Getting the status of the job with the specified job_id.

        There are 2 cases:
            1) Run by Admin: He has access to the status of any job in the system.
            2) Run by User: He has access only to the status of his own jobs in the system.
        """
    user_id = current_user["sub"]
    roles = current_user.get("realm_access", {}).get("roles", [])
    is_admin = "admin" in roles

    job = await orchestrator.get_job_status(job_id=job_id, user_id=user_id, is_admin=is_admin)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or access denied."
        )
    return job


@router.get("/{job_id}/result")
async def get_job_result(
        job_id: str,
        current_user: dict = Depends(get_current_user),
        orchestrator: JobOrchestrator = Depends(get_orchestrator) # <--- Χρήση του Dependency
):
    """
    REST Endpoint for CLI: 'jobs result <id>'.

    1. Validates that the job status is 'COMPLETED'.
    2. Validates ownership (User A cannot download User B's result unless Admin).
    3. Returns a StreamingResponse from MinIO to avoid buffering large files in UI memory.
    """
    user_id = current_user["sub"]
    roles = current_user.get("realm_access", {}).get("roles", [])
    is_admin = "admin" in roles

    # Check if the user has access
    job = await orchestrator.get_job_status(job_id=job_id, user_id=user_id, is_admin=is_admin)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or access denied."
        )

    # Check if the job is "COMPLETED"
    if str(job.status.value) != "COMPLETED" and str(job.status) != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job result is not ready. Current status: {job.status}"
        )

    # Get the data from MiniIO based on the output_path of DDS
    try:
        file_stream = await orchestrator.get_result_stream(job.output_path)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving file from storage: {str(e)}"
        )

    return StreamingResponse(
        file_stream,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename=result_{job_id}.json"}
    )