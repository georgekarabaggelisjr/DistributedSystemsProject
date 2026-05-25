import httpx
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class ManagerRouter:
    def __init__(self, manager_service_dns: str, num_replicas: int = 1):
        # Κατασκευάζουμε το base URL του Manager
        self.base_url = f"http://{manager_service_dns}"

    async def dispatch_job(self, payload: Dict[str, Any], idempotency_key: str, auth_token: str) -> bool:
        """
        Καλεί το POST /internal/schedule του Manager.
        Προωθεί το Token και το Idempotency-Key.
        """
        url = f"{self.base_url}/internal/schedule"
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
            "Authorization": auth_token  # Το token έρχεται έτοιμο ως "Bearer <JWT>"
        }

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload, headers=headers)
                if response.status_code == 202:
                    logger.info(f"Job {payload.get('jobId')} successfully scheduled on Manager.")
                    return True

                logger.error(
                    f"Manager rejected job scheduling. Status: {response.status_code}, Response: {response.text}")
                return False
            except Exception as e:
                logger.error(f"HTTP Error while connecting to Manager schedule endpoint: {str(e)}")
                return False

    async def get_job_status(self, job_id: str, auth_token: str) -> Optional[Dict[str, Any]]:
        """
        Καλεί το GET /internal/jobs/{job_id}/status του Manager.
        """
        url = f"{self.base_url}/internal/jobs/{job_id}/status"
        headers = {"Authorization": auth_token}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    return response.json()  # Επιστρέφει το native snake_case DTO
                return None
            except Exception as e:
                logger.error(f"HTTP Error fetching job status for {job_id}: {str(e)}")
                return None

    async def get_all_jobs(self, auth_token: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Καλεί το GET /internal/jobs του Manager (Paginated).
        """
        url = f"{self.base_url}/internal/jobs"
        headers = {"Authorization": auth_token}
        params = {"limit": limit, "offset": offset}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json()
                return []
            except Exception as e:
                logger.error(f"HTTP Error fetching jobs list from Manager: {str(e)}")
                return []