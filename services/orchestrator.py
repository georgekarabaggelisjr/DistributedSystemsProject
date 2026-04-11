class JobOrchestrator:
    """
    Coordinates the multi-step lifecycle of Job Submission and Retrieval.
    Bridges the API Endpoints with the Storage, Database, and Manager nodes.
    """

    def __init__(self, db_session, storage: StorageClient, router: ManagerRouter):
        self.db = db_session
        self.storage = storage
        self.router = router

    async def process_job_submission(self, user_id: str, data_filename: str, data_bytes: bytes, code_filename: str,
                                     code_bytes: bytes) -> str:
        """
        Executes Step 3.2 (Submit Data, Submit Code & Run Compute Job).

        1. Generates a new UUID for the job.
        2. Uploads `data_bytes` and `code_bytes` to MinIO via StorageClient.
        3. Inserts a new record into the DDS (PostgreSQL) with status 'PENDING'.
        4. Calls the ManagerRouter to dispatch the Job Metadata to a Manager instance.

        Returns:
            str: The newly generated job_id to be returned to the CLI (202 Accepted).
        """
        pass

    async def fetch_job_result_stream(self, user_id: str, job_id: str, is_admin: bool):
        """
        Executes Step 3.3 (Retrieve Result).

        1. Queries DDS to verify ownership (user_id matches OR is_admin is True).
        2. Checks if job status is 'COMPLETED'.
        3. If valid, requests the file stream from MinIO and returns it.
        """
        pass