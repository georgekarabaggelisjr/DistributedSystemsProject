"""
Distributed state reconciliation service.

This module provides background auditing logic to ensure synchronization
between the Redis-based capacity semaphore and the actual state of the
Kubernetes cluster. It prevents 'phantom capacity leaks' where resources
remain reserved in Redis despite the underlying pods having failed or
never being created.

The reconciler enforces full resource cleanup for phantom jobs,
ensuring that abandoned dynamic RabbitMQ queues and Kubernetes metadata
are forcefully purged alongside the Redis capacity locks. Furthermore, it
actively resolves eventual consistency gaps by aggressively transitioning
the persistent PostgreSQL job record to 'FAILED', bypassing the 1-hour
Watchdog sweep.
"""

import asyncio
import logging

from app.core.redis import get_redis_client
from app.core.database import get_db_pool
from app.k8s import client
from app.k8s.client import get_k8s_provider
from app.repositories.cache_repository import CacheRepository
from app.repositories.job_repository import JobRepository
from app.services.lifecycle import forcefully_reclaim_job

logger = logging.getLogger(__name__)


async def run_reconciliation_audit() -> None:
    """
    Performs a point-in-time audit of Redis reservations vs K8s reality.

    Identifies 'Phantom Reservations' (jobs with reserved capacity but 0
    active pods) and forcefully releases the locks to maintain cluster
    availability. It relies on the CacheRepository for state fetching and
    the centralized Lifecycle service for transaction safety.
    """
    redis_client = await get_redis_client()
    k8s_provider = await get_k8s_provider()
    db_pool = await get_db_pool()

    # 1. Ask the DAL for active reservations (No raw Redis keys here)
    active_job_ids = await CacheRepository.get_active_job_reservations(redis_client)

    for job_id in active_job_ids:
        try:
            # 2. Query the source of truth (Kubernetes)
            actual_pod_count = await client.get_active_pod_count(job_id)

            # 3. If Redis claims capacity is used, but K8s is empty, check for grace period
            if actual_pod_count == 0:
                # Ask the DAL for the elapsed time (No raw SQL here)
                elapsed_seconds = await JobRepository.get_job_elapsed_time(job_id, db_pool)

                # Only kill if the job hasn't updated its DB record in the last 60 seconds
                if elapsed_seconds > 60:
                    logger.warning(
                        "Reconciliation: Detected phantom capacity for Job %s. "
                        "Synchronizing Redis state with cluster reality.",
                        job_id
                    )

                    # 4. Delegate to the centralized teardown transaction
                    await forcefully_reclaim_job(job_id, db_pool, k8s_provider, redis_client)

        except Exception as e:
            logger.error("Audit failed for job %s: %s", job_id, str(e))


async def reconciler_loop() -> None:
    """
    Infinite background loop for autonomous system self-healing.

    This loop executes the reconciliation audit periodically. In a multi-manager
    Active-Active architecture, it utilizes a Redis-backed Distributed Lock
    (Leader Election) to ensure that only one manager instance performs the
    Kubernetes API audit per cycle. This prevents race conditions, redundant
    K8s API polling, and overlapping resource reclamation efforts.

    :return: None
    :rtype: None
    """
    logger.info("State Reconciler loop activated (Interval: 600s).")
    while True:
        try:
            redis_client = await get_redis_client()

            # --- LEADER ELECTION (Active-Passive Execution) ---
            # Attempt to acquire a distributed lock.
            # ex=300 ensures the lock auto-expires in 5 minutes if the leader crashes.
            is_leader = await redis_client.set(
                "cluster:reconciler_leader_lock",
                "LOCKED",
                nx=True,
                ex=300
            )

            if is_leader:
                logger.debug("Reconciler acquired leader lock. Executing cluster audit.")
                await run_reconciliation_audit()
            else:
                logger.debug("Reconciler leader lock held by another instance. Bypassing audit.")

        except Exception as e:
            logger.error("Critical failure in Reconciler background loop: %s", str(e))

        # Hibernate for 10 minutes to avoid excessive K8s API pressure
        await asyncio.sleep(600)