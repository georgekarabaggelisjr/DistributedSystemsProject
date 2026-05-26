"""
Service module for message broker interactions and lineage recovery.

This module orchestrates the transition from high-level job definitions to discrete
executable units. It handles the physical partitioning of S3 data sources into
parallelizable byte-range chunks and manages the secure dispatching of task
manifests to the distributed worker cluster via RabbitMQ.

Zero-Trust Data Plane & O(1) Memory Partitioning:
- Integrates cryptographically secure HMAC signatures into every task payload,
  enabling stateless authentication with the External Shuffle Service (ESS).
- Replaces local integer-division partitioning with the Orchestrator's O(1)
  Memory Generator, completely preventing Out-Of-Memory (OOM) crashes when
  generating manifests for multi-terabyte datasets, while strictly enforcing
  configured chunk size limits.
- Utilizes Transient Delivery and Lazy Queues to handle millions of chunk
  manifests entirely in RAM, eliminating severe disk I/O bottlenecks.
- Employs Asynchronous Batch Publishing to decouple payload generation from
  network dispatch, completely eliminating event-loop blocking when flushing
  massive workloads to the broker.
"""

import hmac
import hashlib
import logging
import itertools
import asyncio
import aio_pika
import aio_pika.abc

from app.core.config import settings
from app.models.schemas import TaskMessage, TaskType
from app.services import orchestrator

# Initialize module-level logger for task distribution events
logger = logging.getLogger(__name__)


def _generate_job_token(job_id: str) -> str:
    """
    Generates a secure HMAC-SHA256 token for job authentication.

    Utilizes a shared secret injected via environment variables to create a
    stateless cryptographic signature of the job ID. This token is echoed back
    by workers during shuffle fetches to verify authorization.

    :param job_id: The universally unique identifier of the job to sign.
    :type job_id: str
    :return: A hexadecimal string representation of the HMAC signature.
    :rtype: str
    """
    secret = getattr(settings, "ESS_SECRET_KEY", "dev-insecure-shared-secret").encode("utf-8")
    return hmac.new(secret, job_id.encode("utf-8"), hashlib.sha256).hexdigest()


class TaskPublisher:
    """
    Handles the lifecycle of execution tasks within the distributed message queue.

    This class provides static methods to partition large datasets and (re)publish
    task intents, serving as the primary bridge between the Orchestrator's
    logic and the physical AMQP broker.
    """

    @staticmethod
    async def publish_map_tasks(
        job_id: str,
        total_chunks: int,
        file_size: int,
        s3_input_uri: str,
        mapper_bucket: str,
        mapper_obj: str,
        mapper_class: str,
        num_reducers: int,
        mq_channel: aio_pika.RobustChannel
    ) -> None:
        """
        Calculates data splits and broadcasts Map tasks to the worker cluster.

        Utilizes concurrent batch execution via `asyncio.gather`.
        When publishing massive datasets (e.g., 100,000+ chunks),
        this prevents the Python event loop from being bottlenecked by individual
        network RTTs to the RabbitMQ broker, maximizing network throughput.

        :param job_id: Unique identifier for the MapReduce job.
        :type job_id: str
        :param total_chunks: Number of parallel map tasks to generate.
        :type total_chunks: int
        :param file_size: Total size of the input file in bytes.
        :type file_size: int
        :param s3_input_uri: The full S3 path (s3://bucket/key) of the source data.
        :type s3_input_uri: str
        :param mapper_bucket: S3 bucket containing the user's Mapper bytecode.
        :type mapper_bucket: str
        :param mapper_obj: S3 key for the Mapper bytecode.
        :type mapper_obj: str
        :param mapper_class: Fully qualified name of the Java Mapper class.
        :type mapper_class: str
        :param num_reducers: Target partition count for the shuffle phase.
        :type num_reducers: int
        :param mq_channel: The active RabbitMQ channel for message publication.
        :type mq_channel: aio_pika.RobustChannel
        :raises Exception: If RabbitMQ publication or URI parsing fails.
        :return: None
        :rtype: None
        """
        try:
            data_bucket, data_obj = orchestrator.parse_s3_uri(s3_input_uri)

            # Generate a consistent job token for the entire Map phase
            secure_job_token = _generate_job_token(job_id)

            # --- MULTITENANCY FIX: Declare dynamic queue for this specific job ---
            dynamic_queue_name = f"map_tasks_{job_id}"
            await mq_channel.declare_queue(
                dynamic_queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "dead_letter_exchange",
                    "x-queue-mode": "lazy",
                    "x-expires": 86400000
                }
            )

            # --- PERFORMANCE OPTIMIZATION: O(1) Memory Chunk Generation ---
            # Instantiates the generator to sequentially stream task boundaries
            # rather than buffering thousands of chunk calculations in memory.
            chunk_generator = orchestrator.generate_chunk_splits(file_size)

            BATCH_SIZE = 1000
            publish_batch = []

            for i, (start_byte, byte_length) in enumerate(chunk_generator):
                task_msg = TaskMessage(
                    task_id=f"map-chunk-{i}",
                    job_id=job_id,
                    job_token=secure_job_token,
                    task_type=TaskType.MAP,
                    bucket_name=data_bucket,
                    object_name=data_obj,
                    byte_offset=start_byte,
                    byte_length=byte_length,
                    num_reducers=num_reducers,
                    user_code_bucket=mapper_bucket,
                    user_code_object=mapper_obj,
                    class_name=mapper_class
                )

                # Architecture Update: TRANSIENT delivery mode eliminates heavy disk I/O
                # Queue the coroutine without awaiting it immediately
                publish_coro = mq_channel.default_exchange.publish(
                    aio_pika.Message(
                        body=task_msg.model_dump_json(by_alias=True).encode(),
                        delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT
                    ),
                    routing_key=dynamic_queue_name
                )
                publish_batch.append(publish_coro)

                # Flush the batch to the broker concurrently when threshold is reached
                if len(publish_batch) >= BATCH_SIZE:
                    await asyncio.gather(*publish_batch)
                    publish_batch.clear()
                    logger.debug(f"Broadcasted batch up to task {i + 1}/{total_chunks} to '{dynamic_queue_name}'.")

            # Flush any remaining tasks in the final partial batch
            if publish_batch:
                await asyncio.gather(*publish_batch)
                logger.debug(f"Broadcasted final batch to '{dynamic_queue_name}'.")

            logger.info(f"Successfully published all {total_chunks} Map tasks with Auth Tokens (Transient/Batched).")

        except Exception as e:
            logger.error(f"Failed to publish Map tasks to RabbitMQ: {str(e)}")
            raise

    @staticmethod
    async def republish_single_map_task(
        job_id: str,
        task_id: str,
        total_chunks: int,
        file_size: int,
        s3_input_uri: str,
        mapper_bucket: str,
        mapper_obj: str,
        mapper_class: str,
        num_reducers: int,
        mq_channel: aio_pika.abc.AbstractRobustChannel
    ) -> None:
        """
        Republishes a specific Map task for surgical Lineage Recovery.

        Invoked when a downstream Reducer signals a shuffle fetch failure. This method
        re-calculates the exact byte-range for the missing chunk using the generator
        and dispatches a replacement task to the cluster.

        :param job_id: UUID of the parent job.
        :type job_id: str
        :param task_id: The specific chunk identifier (e.g., 'map-chunk-5').
        :type task_id: str
        :param total_chunks: The original chunk count (used for validation).
        :type total_chunks: int
        :param file_size: Total size of the input file.
        :type file_size: int
        :param s3_input_uri: URI of the source data.
        :type s3_input_uri: str
        :param mapper_bucket: S3 bucket for user code.
        :type mapper_bucket: str
        :param mapper_obj: S3 key for user code.
        :type mapper_obj: str
        :param mapper_class: Java class name.
        :type mapper_class: str
        :param num_reducers: Original reducer count.
        :type num_reducers: int
        :param mq_channel: Active RabbitMQ channel.
        :type mq_channel: aio_pika.RobustChannel
        :raises ValueError: If the task_id format is invalid or out of bounds.
        :raises Exception: If republication fails.
        :return: None
        :rtype: None
        """
        try:
            # Extract index from naming convention (e.g., map-chunk-5 -> 5)
            parts = task_id.split('-')
            if len(parts) < 3 or not parts[-1].isdigit():
                raise ValueError(f"Malformed task_id format: {task_id}. Expected format 'map-chunk-<INT>'.")

            chunk_index = int(parts[-1])

            # Strict boundary validation using the passed total_chunks parameter
            if chunk_index >= total_chunks:
                raise ValueError(f"Target chunk index {chunk_index} exceeds total chunks ({total_chunks}).")

            data_bucket, data_obj = orchestrator.parse_s3_uri(s3_input_uri)

            # PERFORMANCE OPTIMIZATION: O(1) Memory Chunk Generation
            # Fast-forward the generator safely using itertools to isolate the exact
            # byte boundary for the requested target chunk.
            chunk_generator = orchestrator.generate_chunk_splits(file_size)
            try:
                start_byte, byte_length = next(itertools.islice(chunk_generator, chunk_index, None))
            except StopIteration:
                raise ValueError(f"Target chunk index {chunk_index} is out of bounds for the calculated splits.")

            # Re-generate the cryptographic token for auth parity
            secure_job_token = _generate_job_token(job_id)

            dynamic_queue_name = f"map_tasks_{job_id}"
            await mq_channel.declare_queue(
                dynamic_queue_name,
                durable=True,
                arguments={
                    "x-dead-letter-exchange": "dead_letter_exchange",
                    "x-queue-mode": "lazy",
                    "x-expires": 86400000
                }
            )

            task_msg = TaskMessage(
                task_id=task_id,
                job_id=job_id,
                job_token=secure_job_token,
                task_type=TaskType.MAP,
                bucket_name=data_bucket,
                object_name=data_obj,
                byte_offset=start_byte,
                byte_length=byte_length,
                num_reducers=num_reducers,
                user_code_bucket=mapper_bucket,
                user_code_object=mapper_obj,
                class_name=mapper_class
            )

            # Architecture Update: TRANSIENT delivery mode eliminates heavy disk I/O
            await mq_channel.default_exchange.publish(
                aio_pika.Message(
                    body=task_msg.model_dump_json(by_alias=True).encode(),
                    delivery_mode=aio_pika.DeliveryMode.NOT_PERSISTENT
                ),
                routing_key=dynamic_queue_name
            )

            logger.warning(f"Lineage Recovery: Successfully republished targeted Map task '{task_id}' (Transient).")

        except Exception as e:
            logger.error(f"Failed to republish targeted Map task {task_id}: {str(e)}")
            raise