from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Request
from typing import List, Dict, Any
from api.deps import get_current_user, get_orchestrator
from models.schemas import JobResponse
from services.orchestrator import JobOrchestrator

router = APIRouter()

@router.post("/", status_code=202)
async def submit_job(
        request: Request,
        data_file: UploadFile = File(...),
        mapper_file: UploadFile = File(...),
        reducer_file: UploadFile = File(...),
        current_user: dict = Depends(get_current_user),
        orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    user_id = current_user["sub"]
    auth_header = request.headers.get("Authorization")

    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    job_id = await orchestrator.submit_job(
        user_id=user_id,
        auth_token=auth_header,
        data_file=data_file,
        mapper_file=mapper_file,
        reducer_file=reducer_file
    )

    return {
        "job_id": job_id,
        "message": "Job submitted successfully and is pending execution."
    }

@router.get("/", response_model=List[JobResponse])
async def list_user_jobs(
    request: Request,
    current_user: dict = Depends(get_current_user),
    orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    roles = current_user.get("realm_access", {}).get("roles", [])
    is_admin = "admin" in roles

    if is_admin:
        return await orchestrator.get_all_jobs(auth_token=auth_header)
    return await orchestrator.get_user_jobs(auth_token=auth_header)

@router.get("/{job_id}/status", response_model=JobResponse)
async def get_single_job_status(
    job_id: str,
    request: Request,
    current_user: dict = Depends(get_current_user),
    orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    job = await orchestrator.get_job_status(job_id=job_id, auth_token=auth_header)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or access denied."
        )
    return job

@router.get("/{job_id}/result")
async def get_job_result(
        job_id: str,
        request: Request,
        current_user: dict = Depends(get_current_user),
        orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    job = await orchestrator.get_job_status(job_id=job_id, auth_token=auth_header)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or access denied."
        )

    # Το αντικείμενο 'job' πλέον είναι dictionary
    job_status = job.get("status")
    if job_status != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job result is not ready. Current status: {job_status}"
        )

    # Ανακατασκευάζουμε δυναμικά το URI εξόδου που χρησιμοποίησε ο Worker
    user_id = current_user["sub"]
    output_path = f"s3://results/{user_id}/{job_id}/"

    return {
        "job_id": job_id,
        "status": "COMPLETED",
        "storage_type": "MinIO (S3-Compatible)",
        "output_directory_uri": output_path,
        "message": (
            f"The job completed successfully. Output is fragmented into multiple files. "
            f"You can download them from your S3 client or MinIO Browser at prefix: {output_path}"
        )
    }