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

    def submit_job(self, data_file_path: str, mapper_file_path: str, reducer_file_path: str) -> str:
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

        paths = {
            "Data": data_file_path,
            "Mapper": mapper_file_path,
            "Reducer": reducer_file_path
        }
        for name, path in paths.items():
            if not os.path.exists(path):
                typer.secho(f"{name} file not found at: {path}", fg=typer.colors.RED)
                raise FileNotFoundError(f"{name} file not found!")

        try:
            with open(data_file_path, 'rb') as data_f, open(mapper_file_path, 'rb') as mapper_f, open(reducer_file_path, 'rb') as reducer_f:
                files = {
                    'data_file': (os.path.basename(data_file_path), data_f),
                    'mapper_file': (os.path.basename(mapper_file_path), mapper_f),
                    'reducer_file': (os.path.basename(reducer_file_path), reducer_f)
                }

                response = requests.post(url, headers=headers, files=files)
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

    def get_result(self, job_id: str) -> None:
        """
        Executes the `jobs result <id>` workflow.

        Issues an HTTP GET to `/jobs/<id>/result`. The backend verifies the JWT
        for ownership or Admin roles, ensures the status is 'COMPLETED', and then
        returns the JSON payload with the MinIO S3 URI pointing to the output folder.

        Args:
            job_id (str): The ID of the completed job.
        """
        url = f"{self.base_url}/jobs/{job_id}/result"
        headers = self._get_auth_header()

        try:
            # Κάνουμε απλό GET, όχι streaming πλέον
            response = requests.get(url, headers=headers)

            # Αν το backend επιστρέψει HTTP 400 (π.χ. Job not COMPLETED) ή 404,
            # το raise_for_status() θα πετάξει exception που θα πιάσουμε παρακάτω.
            response.raise_for_status()

            # Παίρνουμε το JSON με τις πληροφορίες της τοποθεσίας
            data = response.json()

            # Εμφανίζουμε τα αποτελέσματα με όμορφα χρώματα
            typer.secho("\n Job Completed Successfully!", fg=typer.colors.GREEN, bold=True)
            typer.secho(f" Storage Type: {data.get('storage_type')}", fg=typer.colors.CYAN)
            typer.secho(f" Location (S3 URI): {data.get('output_directory_uri')}", fg=typer.colors.YELLOW, bold=True)
            typer.secho(f"\n  Message:\n{data.get('message')}\n", fg=typer.colors.WHITE)

        except requests.exceptions.RequestException as e:
            # Προσπαθούμε να βρούμε το "detail" από το FastAPI HTTPException (π.χ. "Job result is not ready.")
            if e.response is not None:
                try:
                    error_data = e.response.json()
                    error_detail = error_data.get("detail", str(e))
                    typer.secho(f" Error: {error_detail}", fg=typer.colors.RED)
                except ValueError:
                    # Αν η απάντηση δεν είναι JSON
                    typer.secho(f" Error: {e.response.text}", fg=typer.colors.RED)
            else:
                typer.secho(f" Connection Error: {e}", fg=typer.colors.RED)