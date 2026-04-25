import hashlib
import httpx
import logging

logger = logging.getLogger(__name__)

class ManagerRouter:
    """
    Implements the Consistent Hashing strategy for Job Assignment as defined in Design.md.
    Ensures that the same job_id always routes to the same Manager Service replica.
    """

    def __init__(self, manager_service_dns: str, num_replicas: int):
        """
        Args:
            manager_service_dns: The internal DNS of the headless service (π.χ. 'manager-service.default.svc.cluster.local')
            num_replicas: THe number of replicas of the StatefulSet (π.χ. 3)
        """
        self.manager_dns = manager_service_dns
        self.num_replicas = num_replicas

    def _hash_job_id(self, job_id: str) -> int:
        """
        Applies a consistent hashing algorithm (SHA-256) to the job_id.
        """
        hash_digest = hashlib.sha256(job_id.encode()).hexdigest()
        return int(hash_digest, 16) % self.num_replicas

    async def dispatch_job(self, job_id: str, job_metadata: dict) -> bool:
        """
        Routes the job execution request to the appropriate Manager StatefulSet replica.

        Workflow:
        1. Calculates the hash of the job_id.
        2. Determines the target replica (e.g., 'manager-2.manager-service.default.svc.cluster.local').
        3. Sends an HTTP POST to the internal Manager endpoint with the S3 URIs.

        Returns:
            bool: True if the Manager accepted the job.
        """
        target_index = self._hash_job_id(job_id)

        # Building the internal URL for the specific pod (Structure: pod-name.service-name.namespace.svc.cluster.local)
        target_manager_url = f"http://manager-{target_index}.{self.manager_dns}:8001/jobs/execute"

        logger.info(f"Routing Job {job_id} to Manager Replica {target_index} at {target_manager_url}")

        # Sending HTTP POST to Manager
        async with httpx.AsyncClient(timeout=10.0) as client:
            try:
                # Send metadata (S3 URIs, Job ID, ...)
                response = await client.post(
                    target_manager_url,
                    json={
                        "job_id": job_id,
                        **job_metadata
                    }
                )

                if response.status_code == 202:
                    logger.info(f"Manager-{target_index} accepted job {job_id}")
                    return True
                else:
                    logger.error(f"Manager-{target_index} returned error {response.status_code}: {response.text}")
                    return False

            except httpx.RequestError as exc:
                logger.error(f"Could not connect to Manager-{target_index} at {target_manager_url}: {exc}")
                return False