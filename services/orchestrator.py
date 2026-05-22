import logging
from uuid import uuid4
from typing import List, Optional, AsyncGenerator

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from services.storage_client import StorageClient
from services.manager_router import ManagerRouter
from models.dds_models import JobRecord

logger = logging.getLogger(__name__)

class JobOrchestrator:
    """
    Coordinates the multi-step lifecycle of Job Submission and Retrieval.
    Bridges the API Endpoints with the Storage, Database, and Manager nodes.
    """

    def __init__(self, db_session, storage: StorageClient, router: ManagerRouter):
        self.db = db_session
        self.storage = storage
        self.router = router

    async def submit_job(self,
        user_id: str,
        auth_token: str, # <-- ΝΕΟ: Δέχεται το token
        data_file: UploadFile,
        mapper_file: UploadFile,
        reducer_file: UploadFile
    ) -> str:
        job_id = str(uuid4())
        base_path = f"{user_id}/{job_id}"

        try:
            # (Τα uploads στο storage παραμένουν ως έχουν...)
            data_uri = await self.storage.upload_file("data", f"{base_path}/{data_file.filename}", data_file.file)
            mapper_uri = await self.storage.upload_file("code", f"{base_path}/{mapper_file.filename}", mapper_file.file)
            reducer_uri = await self.storage.upload_file("code", f"{base_path}/{reducer_file.filename}", reducer_file.file)
            output_uri = f"s3://output/{base_path}/results/"

            # (Η εγγραφή στη βάση παραμένει ως έχει...)
            new_job = JobRecord(
                id=job_id,
                user_id=user_id,
                status="PENDING",
                format="JSON", # Μπορείς να το κάνεις δυναμικό αν θες
                input_filename=data_file.filename,
                mapper_code_path=mapper_uri,
                reducer_code_path=reducer_uri,
                output_path=output_uri
            )

            self.db.add(new_job)
            await self.db.commit()

            # --- ΝΕΟ: Το Payload με βάση το POST_REQUEST.md ---
            job_metadata = {
                "jobId": job_id,
                "format": "JSON",
                "inputFilename": data_file.filename,
                "fileSize": data_file.size,
                "mapperFilename": mapper_file.filename,
                "reducerFilename": reducer_file.filename
            }

            # Περνάμε και το token στον Router
            success = await self.router.dispatch_job(job_id, job_metadata, auth_token)

            if not success:
                logger.error(f"Job {job_id} saved but Manager dispatch failed.")

            return job_id

        except Exception as e:
            await self.db.rollback()
            logger.error(f"Failed to submit job {job_id} for user {user_id}: {str(e)}")
            raise e


    async def get_job_status(self, job_id: str, user_id: str, is_admin: bool) -> Optional[JobRecord]:
        """
        Executes Step 3.4 (Check Job Status).

        Validates ownership: Returns the job ONLY if (user_id == owner) OR (is_admin == True).
        """
        query = select(JobRecord).where(JobRecord.id == job_id)

        # If it's not the admin we filter based on the user's id. If it's the admin, we don't filter, because he has access to the status of all the jobs
        if not is_admin:
            query = query.where(JobRecord.user_id == user_id)

        result = await self.db.execute(query)

        return result.scalar_one_or_none() #Returnss the result or none

    async def get_user_jobs(self, user_id: str) -> List[JobRecord]:
        """
        Executes Step 3.5 (User view): Returns all jobs belonging to a specific user.
        """
        query = select(JobRecord).where(JobRecord.user_id == user_id)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_all_jobs(self) -> List[JobRecord]:
        """
        Executes Step 3.5 (Admin view): Returns every job in the system.
        """
        query = select(JobRecord)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_result_stream(self, s3_uri: str) -> AsyncGenerator[bytes, None]:
        """
        Executes Step 3.3 (Retrieve Result).

        Acts as a proxy to StorageClient to stream bytes from MinIO back to the API.
        """
        # Note: An S3 URI is like this: s3://bucket-name/path/to/file.json
        # We need to break it to pieces

        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI format: {s3_uri}")

        uri_parts = s3_uri.replace("s3://", "").split("/")
        bucket_name = uri_parts[0]
        file_path = "/".join(uri_parts[1:])

        # Requesting the stream from the storage client
        return self.storage.get_file_stream(bucket_name, file_path)

    async def _dispatch_to_manager(self, job_id: str, s3_input_uri: str):
        """
        Internal helper to notify the selected Manager instance about the new job.
        """
        await self.router.route_job_to_manager(job_id=job_id, input_uri=s3_input_uri)