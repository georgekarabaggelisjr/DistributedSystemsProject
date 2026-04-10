from core.LocalStateManager import LocalStateManager

import os
import requests
import typer
from core.LocalStateManager import LocalStateManager

class JobClient:
    """
    Handles the submission, monitoring, and retrieval of Map-Reduce jobs.

    All methods in this class require a valid JWT token injected into the
    Authorization header. This client communicates exclusively with the UI Service.
    """

    def __init__(self, state_manager: LocalStateManager):
        self.state_manager = state_manager
        self.base_url = self.state_manager.get_ui_service_url()

    def _get_auth_header(self):
        """Creates the Authorization Header."""
        token = self.state_manager.get_token()
        if not token:
            typer.secho("Token not found. Please login first/again.", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        return {"Authorization": f"Bearer {token}"}

    def submit_job(self, data_file_path: str, code_file_path: str) -> str:
        """
        ========== Implements the 2nd step of the 3.2 diagram =========

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

        url = f"{self.base_url}/jobs"
        headers = self._get_auth_header()

        # Check if the data nad code filepaths exist
        if not os.path.exists(data_file_path):
            typer.secho("Data file not found!", fg=typer.colors.RED)
            raise FileNotFoundError("Data file not found!")

        if not os.path.exists(code_file_path):
            typer.secho("Code file not found!", fg=typer.colors.RED)
            raise FileNotFoundError("Code file not found!")

        try:
            with open(data_file_path, 'rb') as data_file, open(code_file_path, 'rb') as code_file:
                files = {
                    'data_file': (os.path.basename(data_file_path), data_file),
                    'code_file': (os.path.basename(code_file_path), code_file)
                }

                # Step 2 of diagram 3.2 (Sending to the UI service)
                response = requests.post(url, headers=headers, files=files)

                # Checking if the UI service answered with no errors (202 accepted)
                response.raise_for_status()

                data = response.json()
                return data.get("job_id")

        except requests.exceptions.RequestException as e:
            typer.secho(f"Error at POST to UI service: {e}", fg=typer.colors.RED)
            raise typer.Exit(code=1)

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

        if job_id:
            url = f"{self.base_url}/jobs/{job_id}/status"
        else:
            url = f"{self.base_url}/jobs"

        headers = self._get_auth_header()

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            typer.secho(f"Σφάλμα κατά την ανάκτηση κατάστασης: {e}", fg=typer.colors.RED)
            return {}
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
        url = f"{self.base_url}/jobs/{job_id}/result"
        headers = self._get_auth_header()

        try:
            with requests.get(url, headers=headers, stream=True) as r:
                r.raise_for_status()

                # Άνοιγμα τοπικού αρχείου για εγγραφή των bytes
                with open(output_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # φιλτράρισμα keep-alive chunks
                            f.write(chunk)

            typer.secho(f"Result saved at: {output_path}", fg=typer.colors.GREEN)

        except requests.exceptions.RequestException as e:
            typer.secho(f"Error during saving the result: {e}", fg=typer.colors.RED)