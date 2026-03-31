from core.LocalStateManager import LocalStateManager


class JobClient:
    """
    Handles the submission, monitoring, and retrieval of Map-Reduce jobs.

    All methods in this class require a valid JWT token injected into the
    Authorization header. This client communicates exclusively with the UI Service.
    """

    def __init__(self, state_manager: LocalStateManager):
        pass

    def submit_job(self, data_file_path: str, code_file_path: str) -> str:
        """
        Executes the `jobs submit <files>` workflow.

        Constructs a `multipart/form-data` payload containing the input data
        (e.g., JSON/Text) and the executable code (`.jar` or `.class`). It sends
        an HTTP POST to `/jobs`.

        Asynchronous Orchestration:
        The UI Service does not wait for the job to finish. It uploads to MinIO,
        creates a DDS record, and routes to the Manager. This method expects a
        202 Accepted response containing the generated `job_id`.

        Args:
            data_file_path (str): Local path to the input data file.
            code_file_path (str): Local path to the Java executable code.

        Returns:
            str: The unique job ID returned by the UI Service.
        """
        pass

    def get_job_status(self, job_id: str = None) -> dict:
        """
        Executes both the `jobs status <id>` and `jobs status` workflows.

        If a `job_id` is provided, issues an HTTP GET to `/jobs/<id>/status`.
        If omitted, issues an HTTP GET to `/jobs/status` to retrieve all jobs.

        Role-Based Filtering:
        The UI Service utilizes the injected JWT. If the user is an admin, the
        backend DDS query `SELECT * FROM jobs` is executed. If a standard user,
        it filters by the JWT's `sub` claim.

        Args:
            job_id (str, optional): The specific job ID to query.

        Returns:
            dict: A JSON-compatible dictionary containing job statuses.
        """
        pass

    def download_result(self, job_id: str, output_path: str) -> None:
        """
        Executes the `jobs result <id>` workflow.

        Issues an HTTP GET to `/jobs/<id>/result`. The backend verifies the JWT
        for ownership or Admin roles, ensures the status is 'FINISHED', and then
        streams the file from MinIO.

        Stream Processing:
        This method processes the incoming Chunked Transfer-Encoding Byte Stream
        and safely writes it to the local disk at `output_path` without loading
        the entire file into memory.

        Args:
            job_id (str): The ID of the completed job.
            output_path (str): The local file path where the results will be saved.
        """
        pass