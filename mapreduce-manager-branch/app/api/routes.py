"""
API Routing Module for Internal MapReduce Scheduling and Orchestration.

This module serves as the primary entry point for the MapReduce Manager service
to validate, initiate, and oversee the execution lifecycle of distributed, large-scale
data processing workloads. It coordinates complex multi-layered transactions spanning
relational persistence layers (PostgreSQL), in-memory coordination environments (Redis),
high-throughput message fabrics (RabbitMQ), and elastic container clusters (Kubernetes).

Design Principles & Architecture Patterns:
    1. Separation of Concerns (SOC): HTTP routing is strictly decoupled from Data
       Access Layer (DAL) queries, delegating persistence logic to the Repository layer.
    2. Idempotency Gatekeeping: Employs an atomic, distributed locking mechanism via
       Redis to protect down-stream services from duplicate processing anomalies during
       unreliable client network retry phases.
    3. Write-Ahead State Validation (The "1st Guy" Rule): Commits a strict 'SUBMITTED'
       audit log to the relational store before initiating compute or network side effects,
       eliminating orphan or untracked task processing pipelines.
    4. Multi-Tenant Data Isolation: Programmatically constructs immutable path descriptors
       for cloud object storage boundaries using cryptographically secure tenant identifiers.
    5. Decoupled Saga Orchestration: Leverages explicit try-except catch blocks to perform
       automated compensatory cleanup (Saga Rollbacks) across the Kubernetes cluster and
       Redis cache semaphores if infrastructure provisioning fails mid-execution.
"""

import logging
import uuid
from typing import Dict, Any, cast, Optional, List

import aio_pika
import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Header, status
from redis.asyncio import Redis

from app.core.config import settings
from app.core.database import get_db_pool
from app.core.logger import job_id_var
from app.core.rabbitmq import get_rabbitmq_channel
from app.core.redis import get_redis_client
from app.core.auth import get_current_user_id, verify_ui_service
from app.k8s.client import KubernetesProvider, get_k8s_provider
from app.models.schemas import ScheduleJobRequest, JobStatusResponse
from app.repositories.cache_repository import CacheRepository
from app.repositories.job_repository import JobRepository
from app.services import orchestrator
from app.services.lifecycle import forcefully_reclaim_job
from app.services.task_publisher import TaskPublisher

# Initialize structural module-level logging context for execution tracing
logger = logging.getLogger(__name__)
router = APIRouter(tags=["Orchestration"])


@router.post(
    "/internal/schedule",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=Dict[str, Any],
    summary="Initiate Distributed MapReduce Pipeline"
)
async def handle_schedule_job(
        payload: ScheduleJobRequest,
        idempotency_key: str = Header(
            ...,
            description="Unique client-generated token to safely retry scheduling requests without duplication."
        ),
        current_user_id: str = Depends(get_current_user_id),
        db_pool: asyncpg.Pool = Depends(get_db_pool),
        mq_channel: aio_pika.RobustChannel = Depends(get_rabbitmq_channel),
        k8s_provider: KubernetesProvider = Depends(get_k8s_provider),
        redis_client: Redis = Depends(get_redis_client)
) -> Dict[str, Any]:
    """
    Submits, validates, allocates resources for, and runs a new MapReduce job.

    This endpoint transforms a raw input specification file into an operational,
    highly parallelized computation fabric distributed over an elastic cluster.
    Execution handles file splitting metrics, cluster token generation, state transitions,
    K8s pod scheduling, and worker synchronization messages.

    Args:
        payload (ScheduleJobRequest): Validated DTO containing user specifications,
            file sizing bounds, class identifiers, and voluntary capacity overrides.
        idempotency_key (str): Request header token mapping execution uniqueness boundaries.
        current_user_id (str): The cryptographically verified tenant ID extracted from the JWT.
        db_pool (asyncpg.Pool): Shared, multi-tenant relational persistence connection pool.
        mq_channel (aio_pika.RobustChannel): Open AMQP pipeline channel for message distribution.
        k8s_provider (KubernetesProvider): Native engine provider interface managing pod orchestration.
        redis_client (Redis): Distributed in-memory data store managing cluster tracking locks.

    Returns:
        Dict[str, Any]: Execution summary details verifying resource topology metrics:
            - job_id (str): The assigned target unique identifier string.
            - spawned_workers (int): Total active map pods successfully scheduled.
            - num_reducers (int): Explicit or derived reduce engine count.
            - status (str): High-level operational outcome signal ('SUCCESS').

    Raises:
        HTTPException (401): If the provided JWT token is invalid, missing, or expired.
        HTTPException (400): Given invalid, non-conforming worker configurations.
        HTTPException (409): Triggered during concurrent request overlaps under the same key.
        HTTPException (503): Thrown if cluster semaphore registers zero available tenant space.
        HTTPException (500): General systemic or unhandled dependency orchestration failures.
    """
    job_id_str: str = str(payload.job_id)

    # --- SECURITY OVERRIDE ---
    # Completely ignore any user_id sent in the JSON payload to prevent
    # tenants from executing jobs in another user's isolated S3 bucket.
    # Use the cryptographically verified ID from the JWT token instead.
    user_id_str: str = current_user_id

    # Bind thread/async context-local variable for structured JSON log context amplification
    job_id_var.set(job_id_str)

    # --- ATOMIC IDEMPOTENCY LOCK LAYER ---
    # Abstracted through the CacheRepository Data Access Object to protect the core
    # API implementation from direct coupling to underlying Redis command syntax.
    lock_acquired, cached_response = await CacheRepository.try_acquire_idempotency_lock(
        idempotency_key=idempotency_key,
        ttl=settings.IDEMPOTENCY_LOCK_TTL,
        redis_client=redis_client
    )

    if not lock_acquired:
        if cached_response and cached_response.get("status") == "PROCESSING":
            logger.warning("Duplicate execution vector blocked. Request is already actively processing.")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A request with this specific idempotency key is actively being processed within the cluster."
            )
        logger.info("Idempotent hit recorded. Returning cached execution response context.")
        return cast(Dict[str, Any], cached_response)

    # --- MULTI-TENANT STORAGE SANDBOX ENFORCEMENT ---
    # Construct paths strictly referencing the validated user_id token. This guarantees
    # complete multi-tenant boundaries at the storage access layer.
    constructed_input_uri: str = f"s3://data/{user_id_str}/{payload.input_filename}"
    mapper_bucket: str = "code"
    mapper_obj: str = f"{user_id_str}/{payload.mapper_filename}"

    logger.info("Pipeline lifecycle sequence triggered. Target storage URI resolved: %s", constructed_input_uri)

    try:
        # --- PHASE 1: TOPOLOGY CALCULATIONS (IN-MEMORY) ---
        # Derived strictly before database operations to verify input sizing matrices and
        # validate data structures against schema rules (NOT NULL constraints).
        total_chunks: int = orchestrator.calculate_chunks(payload.file_size)

        # Evaluate Reducer Configuration boundaries
        if payload.num_reducers is not None:
            is_valid, reason = orchestrator.validate_reducers(payload.num_reducers)
            if not is_valid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid reducer configuration: {reason}"
                )
            actual_reducers: int = payload.num_reducers
        else:
            # Algorithmic derivation based on total logical chunk requirements
            actual_reducers = orchestrator.calculate_reducers(total_chunks)

        # Evaluate Mapper Capacity Overrides
        actual_mappers: Optional[int] = None
        if payload.num_mappers is not None:
            is_map_valid, map_reason = orchestrator.validate_mappers(payload.num_mappers)
            if not is_map_valid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid mapper configuration: {map_reason}"
                )
            actual_mappers = payload.num_mappers

        # --- PHASE 2: WRITE-AHEAD STATE RECORDING (THE "1ST GUY") ---
        # The primary API layer synchronously saves the payload into PostgreSQL. This guarantees
        # data persistence and provides an audit trail if the broker encounters issues later.
        try:
            await JobRepository.create_job(
                job_id=job_id_str,
                user_id=user_id_str,
                file_format=payload.format,
                input_filename=payload.input_filename,
                mapper_filename=payload.mapper_filename,
                reducer_filename=payload.reducer_filename,
                db_pool=db_pool,
                num_mappers=actual_mappers,
                num_reducers=actual_reducers
            )
        except asyncpg.UniqueViolationError:
            # Secondary protection constraint against double insertions bypassing Redis
            logger.warning("Concurrent insert attempt trapped by primary key database constraint for Job: %s", job_id_str)
            return {"status": "Accepted (Idempotent DB Fallback)", "job_id": job_id_str}

        # --- PHASE 3: CLUSTER RESOURCING & SEMAPHORE ALLOCATION ---
        # Query distributed resource semaphores to calculate optimal container limits
        # without overflowing global capacity blocks.
        ideal_mappers: int = orchestrator.get_optimal_worker_count(
            total_chunks, phase="map", num_reducers=actual_reducers, num_mappers=actual_mappers
        )

        replicas: int = await CacheRepository.reserve_cluster_capacity(
            job_id=job_id_str,
            requested_mappers=ideal_mappers,
            num_reducers=actual_reducers,
            max_capacity=settings.MAX_TOTAL_WORKERS,
            redis_client=redis_client
        )

        if replicas == 0:
            logger.error("Resource allocation denied: Cluster limits saturated (Ceiling: %d)", settings.MAX_TOTAL_WORKERS)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="The execution cluster has reached its multi-tenant limit. Re-submit request when active workloads finish."
            )

        # --- PHASE 4: EXECUTION MUTATION TRANSITION ---
        # Transition the transactional state machine context safely from SUBMITTED to RUNNING
        # inside a single trip database operation using optimistic concurrency checks.
        derived_mapper_class: str = payload.mapper_filename.rsplit('.class', 1)[0]

        await JobRepository.transition_job_to_running(
            job_id=job_id_str,
            total_chunks=total_chunks,
            num_reducers=actual_reducers,
            db_pool=db_pool,
            num_mappers=actual_mappers
        )

        # --- PHASE 5: ELASTIC DEPLOYMENT SAGA BOUNDARY ---
        # Enforce step-by-step containment limits over external side effects. If external deployment
        # steps fail, the system triggers compensating transactions to prevent resource leaks.
        try:
            # Task Segment A: Provision Virtual Computing Layers (Kubernetes Pods)
            logger.info("Spawning %d Map workers. Cluster capacity slice locked successfully.", replicas)
            await k8s_provider.spawn_workers(job_id=job_id_str, replicas=replicas, phase="map")

            # Task Segment B: Publish Task Specifications (RabbitMQ Broker Integration)
            await TaskPublisher.publish_map_tasks(
                job_id=job_id_str,
                total_chunks=total_chunks,
                file_size=payload.file_size,
                s3_input_uri=constructed_input_uri,
                mapper_bucket=mapper_bucket,
                mapper_obj=mapper_obj,
                mapper_class=derived_mapper_class,
                num_reducers=actual_reducers,
                mq_channel=mq_channel
            )

            # Task Segment C: Document Processing State & Commit Safe Results
            response_payload: Dict[str, Any] = {
                "job_id": job_id_str,
                "spawned_workers": replicas,
                "num_reducers": actual_reducers,
                "status": "SUCCESS"
            }

            # Persist response back to cache layer to handle future duplicate request patterns
            await CacheRepository.cache_idempotent_response(
                idempotency_key=idempotency_key,
                response_payload=response_payload,
                ttl=settings.IDEMPOTENCY_CACHE_TTL,
                redis_client=redis_client
            )
            return response_payload

        except Exception as infrastructure_error:
            # SAGA ROLLBACK STRATEGY: Terminate orphaned resources to prevent capacity starvation
            logger.error(
                "Saga Execution Phase Failure: Launching transactional compensation loops for Job %s due to unexpected fault.",
                job_id_str
            )
            await forcefully_reclaim_job(job_id_str, db_pool, k8s_provider, redis_client)
            raise infrastructure_error

    except Exception as e:
        # Erase the transactional tracking lock if processing parameters hit validation issues
        # before saving state variables to disk.
        await CacheRepository.release_idempotency_lock(idempotency_key=idempotency_key, redis_client=redis_client)
        logger.exception("Critical system orchestration boundary exception encountered: %s", str(e))

        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal orchestration error encountered while evaluating distributed resource topologies."
        )


@router.get(
    "/internal/jobs/{job_id}/status",
    status_code=status.HTTP_200_OK,
    response_model=JobStatusResponse,
    summary="Retrieve MapReduce Job Status",
    dependencies=[Depends(verify_ui_service)]
)
async def get_job_status(
        job_id: str,
        db_pool: asyncpg.Pool = Depends(get_db_pool)
) -> Any:
    """
    Fetches the real-time execution status of a specific distributed job.

    This endpoint is strictly protected by a machine-to-machine boundary.
    The injected dependency guarantees that only the internal UI Service can
    access this route, preventing unauthorized direct queries from external clients.

    Args:
        job_id (str): The UUID string representing the target job.
        db_pool (asyncpg.Pool): The injected multi-tenant relational persistence connection pool.

    Returns:
        JobStatusResponse: A DTO containing the job's current status and progress metrics.

    Raises:
        HTTPException (400): If the provided job_id is not a valid UUID format.
        HTTPException (404): If the specified job_id does not exist in the database.
        HTTPException (403): If the requester identity does not match the UI service.
    """
    try:
        # Validate format at the HTTP boundary before querying the DAL
        uuid.UUID(job_id)
    except ValueError:
        logger.warning("Invalid UUID format requested for job status: %s", job_id)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid job_id format. Must be a valid UUID."
        )

    # Delegate data retrieval to the Repository Layer
    record = await JobRepository.get_job_status(job_id, db_pool)

    if not record:
        logger.warning("Job status requested for non-existent job: %s", job_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found."
        )

    return record


@router.get(
    "/internal/jobs",
    status_code=status.HTTP_200_OK,
    response_model=List[JobStatusResponse],
    summary="Retrieve All Job Statuses",
    dependencies=[Depends(verify_ui_service)]
)
async def get_all_jobs(
        limit: int = 100,
        offset: int = 0,
        db_pool: asyncpg.Pool = Depends(get_db_pool)
) -> Any:
    """
    Retrieves a paginated list of all MapReduce jobs and their execution statuses.

    This endpoint relies on the UI Service to handle end-user Role-Based Access
    Control (RBAC). The Manager service only verifies the machine-to-machine identity,
    ensuring the requester possesses the internal 'ui-service' credentials.

    Args:
        limit (int, optional): The maximum number of records to return per page. Defaults to 100.
        offset (int, optional): The number of records to skip for pagination. Defaults to 0.
        db_pool (asyncpg.Pool): The injected multi-tenant relational persistence connection pool.

    Returns:
        List[JobStatusResponse]: A list of DTOs, each representing a job's database record.

    Raises:
        HTTPException (403): If the requester identity does not match the UI service.
    """
    # Delegate data retrieval to the Repository Layer
    return await JobRepository.get_all_jobs_status(limit, offset, db_pool)