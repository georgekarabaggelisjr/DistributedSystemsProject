from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status
from fastapi.responses import StreamingResponse
from typing import List, Optional, Union
from sqlalchemy.ext.asyncio import AsyncSession

from api.deps import get_current_user, get_db, get_orchestrator
from models.schemas import JobResponse
from services.manager_router import ManagerRouter
from services.orchestrator import JobOrchestrator
from services.storage_client import StorageClient
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, status, Request # <-- Πρόσθ

router = APIRouter()


@router.post("/", status_code=202)
async def submit_job(
        request: Request,  # <-- ΝΕΟ: Για να πάρουμε τα headers του HTTP
        data_file: UploadFile = File(...),
        mapper_file: UploadFile = File(...),
        reducer_file: UploadFile = File(...),
        current_user: dict = Depends(get_current_user),
        orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    user_id = current_user["sub"]

    # Παίρνουμε το Token ακριβώς όπως μας το έστειλε το CLI (π.χ. "Bearer eyJhbG...")
    auth_header = request.headers.get("Authorization")

    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    job_id = await orchestrator.submit_job(
        user_id=user_id,
        auth_token=auth_header,  # <-- ΝΕΟ: Το περνάμε στον orchestrator
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
        orchestrator: JobOrchestrator = Depends(get_orchestrator)
):
    """
    REST Endpoint for CLI: 'jobs result <id>'.

    1. Validates that the job status is 'COMPLETED'.
    2. Validates ownership (User A cannot see User B's result unless Admin).
    3. Returns the S3 URI/Link of the MinIO bucket folder containing the output files.
    """
    user_id = current_user["sub"]
    roles = current_user.get("realm_access", {}).get("roles", [])
    is_admin = "admin" in roles

    # 1. Έλεγχος αν το job υπάρχει και αν ανήκει στον χρήστη
    job = await orchestrator.get_job_status(job_id=job_id, user_id=user_id, is_admin=is_admin)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found or access denied."
        )

    # 2. Έλεγχος αν το job έχει ολοκληρωθεί
    if str(job.status.value) != "COMPLETED" and str(job.status) != "COMPLETED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job result is not ready. Current status: {job.status}"
        )

    # 3. Επιστροφή των πληροφοριών τοποθεσίας (JSON αντί για Streaming)
    # Αν γνωρίζεις το external URL του MinIO Console (π.χ. από env variable),
    # θα μπορούσες να κατασκευάσεις και ένα απευθείας HTTP Link για τον browser.
    return {
        "job_id": job_id,
        "status": "COMPLETED",
        "storage_type": "MinIO (S3-Compatible)",
        "output_directory_uri": job.output_path,
        "message": (
            f"The job completed successfully. Output is fragmented into multiple files. "
            f"You can download them from your S3 client or MinIO Browser at prefix: {job.output_path}"
        )
    }