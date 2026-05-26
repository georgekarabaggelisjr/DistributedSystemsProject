"""
Job lifecycle management service.

This module centralizes complex, cross-domain transactions that span multiple
infrastructure layers (Kubernetes, Redis, PostgreSQL). It ensures that tasks
like job teardowns are executed consistently and idempotently across the cluster,
preventing partial state leaks.
"""

import logging
from typing import Any
from redis.asyncio import Redis

from app.k8s.client import KubernetesProvider
from app.repositories.cache_repository import CacheRepository
from app.repositories.job_repository import JobRepository
from app.core.logger import job_id_var

logger = logging.getLogger(__name__)

async def forcefully_reclaim_job(
    job_id: str,
    db_pool: Any,
    k8s_provider: KubernetesProvider,
    redis_client: Redis
) -> None:
    """
    Executes the isolated, idempotent cleanup pipeline for a MapReduce job.

    This function safely tears down physical compute resources, releases
    multi-tenant mathematical capacity limits from the distributed cache,
    and persistently transitions the database state to 'FAILED'. It is designed
    to safely catch and log individual API errors to prevent disruption of
    bulk garbage collection sweeps.

    :param job_id: The UUID of the job to terminate.
    :type job_id: str
    :param db_pool: The asyncpg connection pool for database mutations.
    :type db_pool: Any
    :param k8s_provider: The Kubernetes client to delete physical pods/jobs.
    :type k8s_provider: KubernetesProvider
    :param redis_client: The Redis client to release the global capacity semaphore.
    :type redis_client: Redis
    :return: None
    :rtype: None
    """
    job_id_var.set(job_id)
    logger.warning(f"Initiating centralized resource reclamation for job {job_id}.")

    try:
        # STEP 1: Reclaim physical compute resources FIRST (Idempotent)
        await k8s_provider.delete_job(job_id)

        # STEP 2: Recover allocated multi-tenant mathematical limits safely
        await CacheRepository.release_job_capacity(job_id, redis_client)

        # STEP 3: Mutate persistent state only AFTER successful reclamation
        await JobRepository.mark_job_failed(job_id, db_pool)
        logger.info(f"Job {job_id} persistently transitioned to FAILED.")

    except Exception as cleanup_err:
        logger.error(f"Failed to fully reclaim job {job_id}: {str(cleanup_err)}")