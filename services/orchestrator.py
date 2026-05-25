import logging
from uuid import uuid4
from typing import List, Optional, Dict, Any

from fastapi import UploadFile
from services.storage_client import StorageClient
from services.manager_router import ManagerRouter

logger = logging.getLogger(__name__)

class JobOrchestrator:
    """
    Coordinates Job Submission and Retrieval.
    Acts purely as an API gateway proxy to MinIO and Manager.
    """

    def __init__(self, storage: StorageClient, router: ManagerRouter):
        self.storage = storage
        self.router = router

    async def submit_job(self,
        user_id: str,
        auth_token: str,
        data_file: UploadFile,
        mapper_file: UploadFile,
        reducer_file: UploadFile
    ) -> str:
        job_id = str(uuid4())
        idempotency_key = str(uuid4())  # Παραγωγή μοναδικού κλειδιού για το δίκτυο

        try:
            # ==========================================
            # 1. Ανέβασμα Αρχείων στο MinIO (Buckets: data, code)
            # ==========================================
            await self.storage.upload_file(
                bucket_name="data",
                file_path=f"{user_id}/{data_file.filename}",
                file_obj=data_file.file
            )

            await self.storage.upload_file(
                bucket_name="code",
                file_path=f"{user_id}/{mapper_file.filename}",
                file_obj=mapper_file.file
            )

            await self.storage.upload_file(
                bucket_name="code",
                file_path=f"{user_id}/{reducer_file.filename}",
                file_obj=reducer_file.file
            )

            # ==========================================
            # 2. Προετοιμασία Payload σε camelCase για τον Manager
            # ==========================================
            job_metadata = {
                "jobId": job_id,
                "format": "TEXT",  # Default format, άλλαξέ το αν χρειάζεται δυναμικά
                "inputFilename": data_file.filename,
                "fileSize": data_file.size if data_file.size else 0,
                "mapperFilename": mapper_file.filename,
                "reducerFilename": reducer_file.filename
            }

            # 3. Αποστολή στον Manager
            success = await self.router.dispatch_job(
                payload=job_metadata,
                idempotency_key=idempotency_key,
                auth_token=auth_token
            )

            if not success:
                raise RuntimeError("Manager rejected the job submission request.")

            return job_id

        except Exception as e:
            logger.error(f"Failed to submit job {job_id} for user {user_id}: {str(e)}")
            raise e

    async def get_job_status(self, job_id: str, auth_token: str) -> Optional[Dict[str, Any]]:
        """Προωθεί το αίτημα ελέγχου κατάστασης στον Manager"""
        return await self.router.get_job_status(job_id=job_id, auth_token=auth_token)

    async def get_user_jobs(self, auth_token: str) -> List[Dict[str, Any]]:
        """Ο Manager απομονώνει αυτόματα τα jobs του χρήστη από το JWT Token"""
        return await self.router.get_all_jobs(auth_token=auth_token)

    async def get_all_jobs(self, auth_token: str) -> List[Dict[str, Any]]:
        """Ο Admin βλέπει όλα τα jobs (το RBAC φιλτράρεται στο route)"""
        return await self.router.get_all_jobs(auth_token=auth_token)