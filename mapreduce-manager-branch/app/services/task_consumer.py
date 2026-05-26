"""
Asynchronous worker event consumer module for MapReduce orchestration.

This module provides the core event-driven logic for tracking MapReduce job
progress and orchestrating phase transitions. It acts as the central coordinator
that consumes completion signals from RabbitMQ and manages the execution barriers
between the Map and Reduce phases.

The processor implements several enterprise patterns:
    * **Stateful Barriers:** Utilizes Redis Lua scripting for atomic task counting.
    * **Data Locality Scheduling:** Pins Reducer pods to physical nodes via K8s affinity.
    * **Reactive Lineage Recovery:** Automatically recomputes lost Map chunks.
    * **Zero-Trust Security:** Generates HMAC tokens to authorize shuffle transfers.
    * **Dynamic Binding:** Derives Java execution classes directly from filenames
      at runtime to minimize payload sizes and database redundancy.
"""

import asyncio
import logging
import hmac
import hashlib
from typing import List, Optional

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from app.core.database import get_db_pool
from app.core.logger import job_id_var
from app.core.rabbitmq import get_rabbitmq_channel
from app.core.redis import get_redis_client
from app.k8s.client import get_k8s_provider
from app.models.schemas import WorkerSignal, TaskMessage
from app.models.enums import TaskType
from app.repositories.cache_repository import CacheRepository
from app.repositories.job_repository import JobRepository
from app.services.task_publisher import TaskPublisher
from app.services.storage import StorageService
from app.core.config import settings
from app.services.lifecycle import forcefully_reclaim_job

logger = logging.getLogger(__name__)


class TaskEventProcessor:
    """
    Handles the ingestion and routing of execution signals from distributed workers.

    This class provides static methods to process asynchronous messages from RabbitMQ,
    interfacing with Kubernetes, PostgreSQL, and Redis to advance the global
    job state machine.
    """

    @classmethod
    async def _ping_db_throttled(cls, job_id_str: str) -> None:
        """
        Throttles PostgreSQL activity updates using a Redis debounce lock.

        In high-throughput scenarios, thousands of concurrent Map tasks sending
        'COMPLETED' or 'IN_PROGRESS' signals can saturate the database with
        update operations. This method employs a 30-second distributed lock
        to ensure the DB is pinged just enough to satisfy the Watchdog timeout,
        eliminating massive I/O bottlenecks without risking job termination.
        """
        redis_client = await get_redis_client()
        lock_key = f"job:{job_id_str}:db_ping_throttle"

        if await redis_client.set(lock_key, "1", nx=True, ex=30):
            db_pool = await get_db_pool()
            await JobRepository.ping_job_activity(job_id_str, db_pool)

    @classmethod
    async def process_message(cls, message: AbstractIncomingMessage) -> None:
        """
        Main entry point for processing worker execution signals.

        Performs JSON validation and routes the signal based on execution state.
        Implements a Hybrid Retry Architecture:
          - Fail-Fast for deterministic user bytecode errors.
          - 3-Strike Bounded Retry for transient infrastructure faults.
          - Continuous Heartbeat ('IN_PROGRESS') for massive data streams to
            prevent premature Watchdog termination.
        """
        async with message.process(ignore_processed=True):
            try:
                raw_data = message.body.decode('utf-8')
                payload = WorkerSignal.model_validate_json(raw_data)
            except (UnicodeDecodeError, ValidationError) as e:
                logger.error("Failed to decode worker completion message: %s", str(e))
                await message.reject(requeue=False)
                return

            job_id_str = str(payload.job_id)
            job_id_var.set(job_id_str)
            logger.info("Received signal for task %s (Status: %s)", payload.task_id, payload.status)

            # --- ARCHITECTURAL FIX: Long-Running Task Heartbeats ---
            if payload.status == "IN_PROGRESS":
                await cls._ping_db_throttled(job_id_str)
                return

            # --- Error Handling & Hybrid Retry Routing ---
            if payload.status != "COMPLETED":

                # 1. Lineage Recovery (Surgical Shuffle Healing)
                if payload.error and payload.error.startswith("SHUFFLE_FETCH_FAILED:"):
                    lost_task_id = payload.error.split(":")[1]
                    logger.warning("Reactive Lineage Recovery Triggered for '%s'.", lost_task_id)

                    redis_client = await get_redis_client()

                    # Bounded Retry for Lineage Recovery (Prevents Infinite Loop)
                    recovery_strikes = await CacheRepository.increment_task_failure(
                        job_id_str, f"recovery_attempts_{lost_task_id}", redis_client
                    )

                    if recovery_strikes >= 3:
                        logger.error("Poison Pill Triggered: Lineage Recovery failed 3 times for %s.", lost_task_id)
                        db_pool = await get_db_pool()
                        k8s_provider = await get_k8s_provider()

                        # Centralized lifecycle teardown
                        await forcefully_reclaim_job(job_id_str, db_pool, k8s_provider, redis_client)

                        await message.reject(requeue=False)
                        return

                    await CacheRepository.add_to_recovery_waitlist(
                        job_id_str, lost_task_id, payload.task_id, redis_client
                    )

                    # Thundering Herd Protection (Prevents Duplicate K8s Spawns)
                    recovery_lock_key = f"job:{job_id_str}:recovering:{lost_task_id}"
                    if await redis_client.set(recovery_lock_key, "LOCKED", nx=True, ex=300):
                        await cls._recover_lost_map_task(job_id_str, lost_task_id)
                    else:
                        logger.debug("Recovery already in progress for %s. Skipping duplicate spawn.", lost_task_id)

                    return

                # 2. Smart Retry Loop: Detect Deterministic vs. Transient Errors
                deterministic_errors = ["NullPointerException", "ClassCastException", "OutOfMemoryError",
                                        "Exception in User Code"]
                is_user_bug = any(err in (payload.error or "") for err in deterministic_errors)

                redis_client = await get_redis_client()

                if is_user_bug:
                    logger.error("Fail-Fast Triggered: Deterministic user error. Skipping retries.")
                    strikes = 3  # Force immediate Poison Pill teardown
                else:
                    # Infrastructure error -> Apply the 3-strike bounded retry
                    strikes = await CacheRepository.increment_task_failure(
                        job_id_str, payload.task_id, redis_client
                    )

                if strikes < 3:
                    logger.warning(
                        "Task %s failed (Strike %d/3). Error: %s. Provisioning replacement compute.",
                        payload.task_id, strikes, payload.error
                    )
                    await message.reject(requeue=True)

                    # Provision 1 exact replacement pod
                    k8s_provider = await get_k8s_provider()
                    target_phase = "map" if "map" in payload.task_id.lower() else "reduce"
                    await k8s_provider.spawn_workers(job_id=job_id_str, replicas=1, phase=target_phase)
                    return

                # 3. Poison Pill Teardown (Strike 3 or User Code Bug)
                logger.error("Poison Pill Triggered. Task %s failed terminally. Error: %s", payload.task_id,
                             payload.error)
                db_pool = await get_db_pool()
                k8s_provider = await get_k8s_provider()

                # Centralized lifecycle teardown
                await forcefully_reclaim_job(job_id_str, db_pool, k8s_provider, redis_client)

                await message.reject(requeue=False)
                return

            # --- Standard Completion Pipeline ---
            await cls._ping_db_throttled(job_id_str)

            if "map" in payload.task_id.lower():
                await cls._handle_map_completion(payload)
            elif "reduce" in payload.task_id.lower() or payload.task_id.isdigit():
                await cls._handle_reduce_completion(payload)
            else:
                logger.error("Unable to route signal: unknown task_id format '%s'", payload.task_id)

    @classmethod
    async def _recover_lost_map_task(cls, job_id: str, lost_task_id: str) -> None:
        """
        Reconstructs execution metadata to re-run a specific Map task.

        Architecture Update:
        Constructs multi-tenant S3 URIs dynamically utilizing the user's UUID.
        Derives the mapper class directly from the persisted filename string.
        """
        logger.info("Initiating Lineage Recovery sequence for %s.", lost_task_id)
        db_pool = await get_db_pool()
        mq_channel = await get_rabbitmq_channel()
        k8s_provider = await get_k8s_provider()

        record = await JobRepository.get_job_recovery_context(job_id, db_pool)

        if not record:
            logger.error("Lineage Recovery failed: Job context not found for %s", job_id)
            return

        try:
            user_id_str = str(record['user_id'])

            # Resolve physical file size for byte-offset calculation
            data_bucket = "data"
            data_obj = f"{user_id_str}/{record['input_filename']}"
            minio_client = StorageService.get_client()
            file_size = minio_client.stat_object(data_bucket, data_obj).size

            mapper_bucket = "code"
            mapper_obj = f"{user_id_str}/{record['mapper_filename']}"
            constructed_input_uri = f"s3://{data_bucket}/{data_obj}"

            # --- DYNAMIC CLASS DERIVATION ---
            derived_mapper_class = record['mapper_filename'].rsplit('.class', 1)[0]

            # Re-provision compute for the recovered task
            logger.info("Re-provisioning 1 Map worker for lineage recovery.")
            await k8s_provider.spawn_workers(
                job_id=job_id,
                replicas=1,
                phase="map"
            )

            # Dispatch recovery task to RabbitMQ
            await TaskPublisher.republish_single_map_task(
                job_id=job_id, task_id=lost_task_id, total_chunks=record['total_chunks'],
                file_size=file_size, s3_input_uri=constructed_input_uri, # type: ignore
                mapper_bucket=mapper_bucket, mapper_obj=mapper_obj,
                mapper_class=derived_mapper_class, num_reducers=record['num_reducers'],
                mq_channel=mq_channel
            )
        except Exception as e:
            logger.error("Failed to execute lineage recovery: %s", str(e))
            redis_client = await get_redis_client()

            # Centralized lifecycle teardown
            await forcefully_reclaim_job(job_id, db_pool, k8s_provider, redis_client)

    @classmethod
    async def process_orchestration_event(cls, message: AbstractIncomingMessage) -> None:
        """
        Processes internal transition events to trigger phase changes.

        This method listens on the orchestration queue, ensuring that the
        Map-to-Reduce transition happens in a clean, decoupled execution context.
        It safely swaps out the Redis global capacity locks from the Map phase
        to the requested capacity for the Reduce phase, preventing cluster
        starvation or pod quota breaches.
        """
        async with message.process(ignore_processed=True):
            try:
                job_id = message.body.decode('utf-8')
                job_id_var.set(job_id)
            except Exception:
                await message.reject(requeue=False)
                return

            db_pool = await get_db_pool()
            metadata = await JobRepository.get_job_metadata(job_id, db_pool)

            if not metadata:
                await message.reject(requeue=False)
                return

            # --- Active Phase Garbage Collection ---
            logger.info("Map phase complete. Actively tearing down Map pods to free capacity.")
            k8s_provider = await get_k8s_provider()
            redis_client = await get_redis_client()

            # STEP 1: Teardown physical K8s Map pods FIRST
            await k8s_provider.delete_job(job_id)

            # STEP 2: Free up the global Map capacity from the semaphore safely
            await CacheRepository.release_job_capacity(job_id, redis_client)

            # --- Prevent Reduce Phase Capacity Hijack ---
            # Re-reserve capacity specifically for the Reduce phase
            # This ensures the Reducers don't breach the MAX_TOTAL_WORKERS quota
            # while accurately locking only the exact amount of capacity they need.
            target_reducers = metadata['num_reducers']

            granted_capacity = await CacheRepository.reserve_cluster_capacity(
                job_id=job_id,
                requested_mappers=target_reducers,  # Mapping to the reservation logic
                num_reducers=target_reducers,
                max_capacity=settings.MAX_TOTAL_WORKERS,
                redis_client=redis_client
            )

            # If the cluster is too saturated to grant the required Reduce capacity,
            # release any partial lock and yield the transition back to the queue.
            if granted_capacity < target_reducers:
                logger.warning(
                    "Capacity Starvation: Could not reserve %d slots for job %s. Requeueing event.",
                    target_reducers, job_id
                )
                await CacheRepository.release_job_capacity(job_id, redis_client)
                await message.reject(requeue=True)
                return
            # -----------------------------------------------------

            # --- DYNAMIC CLASS DERIVATION ---
            derived_reducer_class = metadata['reducer_filename'].rsplit('.class', 1)[0]

            await cls._trigger_reduce_phase(
                job_id=job_id,
                user_id=str(metadata['user_id']),
                reducer_filename=metadata['reducer_filename'],
                reducer_class=derived_reducer_class,
                num_reducers=target_reducers
            )

    @classmethod
    async def _handle_map_completion(cls, payload: WorkerSignal) -> None:
        """
        Manages the synchronization barrier for the Map phase.

        Records worker endpoints for shuffle fetches and checks if all Map
        tasks are complete. If the barrier is reached, it triggers the
        orchestration event to move to the Reduce phase. Includes logic to
        intercept and heal stalled Reducers during Lineage Recovery.
        """
        redis_client = await get_redis_client()
        db_pool = await get_db_pool()
        job_id_str = str(payload.job_id)

        # Lazy-loading and caching total chunks in Redis
        total = await CacheRepository.get_cached_total_chunks(job_id_str, redis_client)
        if total is None:
            metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
            if not metadata:
                return
            total = metadata['total_chunks']
            await CacheRepository.cache_total_chunks(job_id_str, total, redis_client)

        # Store the physical location (ESS endpoint) for the Reducer phase
        if payload.worker_bind_address:
            await CacheRepository.add_worker_endpoint(
                job_id=job_id_str, task_id=payload.task_id,
                endpoint=payload.worker_bind_address, redis_client=redis_client
            )

        # Lineage Recovery Interceptor
        waiting_reducer_ids: List[str] = await CacheRepository.pop_from_recovery_waitlist(
            job_id_str, payload.task_id, redis_client
        )

        if waiting_reducer_ids:
            logger.info(f"Lineage Recovery successful for {payload.task_id}. Waking up Reducers: {waiting_reducer_ids}")

            # Re-fetch metadata required to spawn the Reducers
            metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
            if not metadata:
                return

            # --- DYNAMIC CLASS DERIVATION ---
            derived_reducer_class = metadata['reducer_filename'].rsplit('.class', 1)[0]

            # Iteratively spawn each specific partition that failed
            for reducer_id in waiting_reducer_ids:
                await cls._trigger_reduce_phase(
                    job_id=job_id_str,
                    user_id=str(metadata['user_id']),
                    reducer_filename=metadata['reducer_filename'],
                    reducer_class=derived_reducer_class,
                    num_reducers=metadata['num_reducers'],
                    target_partition_id=int(reducer_id)
                )
            return

        # Atomic barrier check using Redis Lua
        is_barrier_breached = await CacheRepository.register_map_completion_atomic(
            job_id_str, payload.task_id, total, redis_client
        )

        if is_barrier_breached:
            # Use an NX lock to ensure only one consumer triggers the transition
            transition_lock_key = f"job:{job_id_str}:reduce_transition_fired"
            if not await redis_client.set(transition_lock_key, "1", nx=True, ex=86400):
                return

            logger.info("MAP BARRIER REACHED (Atomic Lock Acquired). Publishing transition event.")
            await JobRepository.flush_map_phase(job_id_str, total, db_pool)
            mq_channel = await get_rabbitmq_channel()
            # Retain PERSISTENT delivery for phase transition to survive broker outages
            await mq_channel.default_exchange.publish(
                aio_pika.Message(body=job_id_str.encode('utf-8'), delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                routing_key="orchestration_events_queue"
            )

    @classmethod
    async def _trigger_reduce_phase(
            cls,
            job_id: str,
            user_id: str,
            reducer_filename: str,
            reducer_class: str,
            num_reducers: int,
            target_partition_id: Optional[int] = None
    ) -> None:
        """
        Orchestrates the deployment of Reducer workers with Data Locality logic.

        Architecture Update:
        Targets the multi-tenant `results` and `code` buckets directly using the
        injected user_id rather than relying on legacy parsed URIs. Output paths are
        sandboxed per user and job ID: `s3://results/<user_id>/<job_id>/`.
        Applies Lazy Queue and Auto-Expiry optimizations dynamically.
        """
        mq_channel = await get_rabbitmq_channel()
        k8s_provider = await get_k8s_provider()
        redis_client = await get_redis_client()

        raw_endpoints = await CacheRepository.get_worker_endpoints(job_id, redis_client)

        # DATA LOCALITY PARSING
        unique_ess_endpoints = list(set(raw_endpoints))
        available_nodes = []
        clean_grpc_endpoints = []

        for ep in unique_ess_endpoints:
            if "@@" in ep:
                node_name, ip_port = ep.split("@@", 1)
                available_nodes.append(node_name)
                clean_grpc_endpoints.append(ip_port)
            else:
                clean_grpc_endpoints.append(ep)

        logger.info("Extracted %d physical Node Hostnames for Data Locality Scheduling.", len(available_nodes))

        # Multi-Tenant Construction
        code_bucket = "code"
        code_obj = f"{user_id}/{reducer_filename}"
        class_name = reducer_class if reducer_class else "WordCountReducer"

        # Generate HMAC-SHA256 token for secure P2P shuffle authentication
        secure_job_token = hmac.new(
            getattr(settings, "ESS_SECRET_KEY", "dev-insecure-shared-secret").encode("utf-8"),
            job_id.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        # Architecture Update: Enforce Lazy Mode and Expiry here as well
        dynamic_queue_name = f"reduce_tasks_{job_id}"
        await mq_channel.declare_queue(
            dynamic_queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "dead_letter_exchange",
                "x-queue-mode": "lazy",
                "x-expires": 86400000
            }
        )

        try:
            async def _provision_single_partition(p_id: int) -> None:
                if target_partition_id is not None and p_id != target_partition_id:
                    return

                target_node = available_nodes[p_id % len(available_nodes)] if available_nodes else None

                await k8s_provider.spawn_workers(
                    job_id=job_id,
                    replicas=1,
                    phase="reduce",
                    partition_id=p_id,
                    target_node=target_node
                )

                # Construct and publish the secure TaskMessage for the Java worker
                reduce_msg = TaskMessage(
                    task_id=str(p_id),
                    job_id=job_id,
                    job_token=secure_job_token,
                    task_type=TaskType.REDUCE,
                    bucket_name="results",                     # Multi-Tenant isolation
                    object_name=f"{user_id}/{job_id}/",        # Output sandboxed per job
                    byte_offset=0,
                    byte_length=0,
                    num_reducers=num_reducers,
                    user_code_bucket=code_bucket,              # Tenant code location
                    user_code_object=code_obj,
                    class_name=class_name,
                    worker_endpoints=clean_grpc_endpoints
                )

                # Architecture Update: TRANSIENT delivery mode eliminates heavy disk I/O
                await mq_channel.default_exchange.publish(
                    aio_pika.Message(
                        body=reduce_msg.model_dump_json(by_alias=True).encode(),
                        delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT
                    ),
                    routing_key=dynamic_queue_name
                )

            provisioning_tasks = [_provision_single_partition(partition_id) for partition_id in range(num_reducers)]
            await asyncio.gather(*provisioning_tasks)

            if target_partition_id is not None:
                logger.info("Successfully provisioned targeted Reducer partition %d for Lineage Recovery.",
                            target_partition_id)
            else:
                logger.info("Successfully provisioned %d targeted Reducers concurrently.", num_reducers)

        except Exception as publish_error:
            logger.error("Saga Triggered: Task publishing failed. Tearing down pods.")
            db_pool = await get_db_pool()

            # Centralized lifecycle teardown
            await forcefully_reclaim_job(job_id, db_pool, k8s_provider, redis_client)

            raise publish_error

    @classmethod
    async def _handle_reduce_completion(cls, payload: WorkerSignal) -> None:
        """
        Manages the final synchronization barrier for the Reduce phase.

        When the final partition is reported as COMPLETED, this method updates
        PostgreSQL to 'COMPLETED', monitors K8s for pod termination, and
        releases job capacity back to the global pool.
        """
        redis_client = await get_redis_client()
        db_pool = await get_db_pool()
        k8s_provider = await get_k8s_provider()
        job_id_str = str(payload.job_id)

        metadata = await JobRepository.get_job_metadata(job_id_str, db_pool)
        if not metadata:
            return
        target_reducers = metadata['num_reducers']

        is_barrier_breached = await CacheRepository.register_reduce_completion_atomic(
            job_id_str, payload.task_id, target_reducers, redis_client
        )

        if is_barrier_breached:
            # Distributed lock to prevent duplicate teardown deadlocks
            completion_lock_key = f"job:{job_id_str}:reduce_completion_fired"
            if not await redis_client.set(completion_lock_key, "1", nx=True, ex=86400):
                logger.debug("Reduce completion barrier already breached. Skipping duplicate teardown.")
                return

            logger.info("FINAL BARRIER REACHED. MapReduce job execution successful.")
            await JobRepository.flush_reduce_phase(job_id_str, target_reducers, db_pool)
            await JobRepository.mark_job_completed(job_id_str, db_pool)

            # Wait for K8s API to confirm all worker processes have exited
            await k8s_provider.wait_for_job_completion(job_id_str)
            await k8s_provider.delete_job(job_id_str)

            # Release Redis quota to allow new submissions
            await CacheRepository.release_job_capacity(job_id_str, redis_client)


async def start_consumer() -> None:
    """
    Bootstraps RabbitMQ event listeners and registers asynchronous callbacks.

    Establishes consumption for:
        1. 'job_events_queue': Worker signals (Map/Reduce progress).
        2. 'orchestration_events_queue': Internal phase transition signals.

    In an Active-Active multi-manager architecture, this function configures
    a QoS prefetch limit to enforce the 'Competing Consumers' pattern, ensuring
    fair message dispatch and preventing memory hoarding by a single instance.
    """
    logger.info("Starting Consumers. Listening on 'job_events_queue' & 'orchestration_events_queue'...")
    channel = await get_rabbitmq_channel()

    await channel.set_qos(prefetch_count=100)

    events_queue = await channel.declare_queue("job_events_queue", durable=True)
    await events_queue.consume(TaskEventProcessor.process_message)

    orch_queue = await channel.declare_queue("orchestration_events_queue", durable=True)
    await orch_queue.consume(TaskEventProcessor.process_orchestration_event)

    logger.info("Consumers are now active and awaiting payloads.")