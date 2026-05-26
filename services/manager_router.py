import httpx
import logging
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

class ManagerRouter:
    def __init__(self, manager_service_dns: str, num_replicas: int = 1):
        self.base_url = f"http://{manager_service_dns}"

    def _format_auth_header(self, token: str) -> str:
        """Helper για να διασφαλίσουμε ότι το token έχει το πρόθεμα 'Bearer '."""
        if not token.startswith("Bearer "):
            return f"Bearer {token}"
        return token

    async def dispatch_job(self, payload: Dict[str, Any], idempotency_key: str, auth_token: str) -> bool:
        url = f"{self.base_url}/internal/schedule"
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
            "Authorization": self._format_auth_header(auth_token) # Χρήση του helper!
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
        url = f"{self.base_url}/internal/jobs/{job_id}/status"
        headers = {"Authorization": self._format_auth_header(auth_token)} # Χρήση του helper!

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers)
                if response.status_code == 200:
                    return response.json()
                logger.error(f"Manager returned {response.status_code} for job {job_id}: {response.text}") # Πρόσθεσα καλύτερο error logging
                return None
            except Exception as e:
                logger.error(f"HTTP Error fetching job status for {job_id}: {str(e)}")
                return None

    async def get_all_jobs(self, auth_token: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/internal/jobs"
        headers = {"Authorization": self._format_auth_header(auth_token)} # Χρήση του helper!
        params = {"limit": limit, "offset": offset}

        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url, headers=headers, params=params)
                if response.status_code == 200:
                    return response.json()
                logger.error(f"Manager returned {response.status_code} for get_all_jobs: {response.text}") # Πρόσθεσα καλύτερο error logging
                return []
            except Exception as e:
                logger.error(f"HTTP Error fetching jobs list from Manager: {str(e)}")
                return []