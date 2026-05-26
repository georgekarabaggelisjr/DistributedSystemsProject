"""
Message broker lifecycle and dependency injection module.

This module manages the asynchronous connection and channel lifecycle for
RabbitMQ using the `aio-pika` library. It implements a robust singleton
pattern to provide persistent communication channels across the Manager's
distributed orchestration tasks.

The module ensures that the Dead Letter Exchange (DLX) topology is
declared and durable. Queue declaration is handled dynamically per job
to prevent cross-talk between distributed worker nodes, now optimized with
lazy paging and auto-expiry mechanisms for enterprise-scale workloads.
"""

import logging
from typing import Any, Dict, Optional

import aio_pika
import aio_pika.abc
from app.core.config import settings

# Initialize module-level logger for broker events
logger = logging.getLogger(__name__)

# Private module-level variables to maintain singleton connection states.
# Using abc types to resolve "got Abstract... instead" linter warnings.
_global_mq_connection: Optional[aio_pika.abc.AbstractRobustConnection] = None
_global_mq_channel: Optional[aio_pika.abc.AbstractRobustChannel] = None


async def setup_rabbitmq() -> None:
    """
    Establishes robust broker connections and declares the DLX topology.

    This function utilizes `connect_robust`, which automatically handles
    reconnections in the event of a broker outage. It ensures that the
    underlying Dead Letter Exchange fabric is ready to safely capture
    failed tasks.

    :return: None
    :rtype: None
    :raises Exception: If the initial connection to the RabbitMQ broker fails
        or if exchange declaration is rejected by the server.
    """
    global _global_mq_connection, _global_mq_channel

    logger.info("Connecting to RabbitMQ broker...")
    try:
        # connect_robust handles automatic heartbeats and connection recovery.
        connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
        _global_mq_connection = connection

        channel = await connection.channel()
        _global_mq_channel = channel

        # --- DLX TOPOLOGY SETUP ---
        logger.info("Declaring Dead Letter Exchange (DLX) topology...")

        # 1. Declare the DLX (Direct Exchange) where rejected messages are routed
        dlx_exchange = await channel.declare_exchange(
            "dead_letter_exchange",
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        # 2. Configure autonomous cleanup for the quarantine zone
        # Messages older than 7 days will be purged, and the queue is capped at 10k messages.
        dlq_args: Dict[str, Any] = {
            "x-message-ttl": 604800000,  # 7 days in milliseconds
            "x-max-length": 10000        # Safety cap to prevent memory exhaustion
        }

        # 3. Declare the actual Dead Letter Queue (DLQ) to hold failed tasks
        dlq = await channel.declare_queue(
            "dead_letter_queue",
            durable=True,
            arguments=dlq_args
        )

        logger.info("RabbitMQ connection established and DLX initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize RabbitMQ: {str(e)}")
        raise


async def declare_job_queues(channel: aio_pika.abc.AbstractRobustChannel, job_id: str) -> None:
    """
    Dynamically declares isolated task queues for a specific MapReduce job.

    This ensures a strict Job-Specific Queue architecture, preventing
    worker cross-talk and guaranteeing data isolation. The queues are
    automatically bound to the global Dead Letter Exchange.

    Architecture Updates:
        - Lazy Mode: Forces messages to disk as fast as possible, keeping RAM
          usage O(1) even if a job generates millions of chunk manifests.
        - Auto-Expiry: Queues unused for 24 hours self-destruct, preventing
          broker metadata leaks if the Orchestrator crashes during teardown.

    :param channel: The active RabbitMQ channel to declare queues on.
    :type channel: aio_pika.abc.AbstractRobustChannel
    :param job_id: The unique identifier for the job, used as a suffix.
    :type job_id: str
    :return: None
    :rtype: None
    """
    queue_args = {
        "x-dead-letter-exchange": "dead_letter_exchange",
        "x-queue-mode": "lazy",  # Maintain flat RAM usage during massive data inputs
        "x-expires": 86400000    # 24-hour autonomous garbage collection (in milliseconds)
    }

    map_queue_name = f"map_tasks_{job_id}"
    reduce_queue_name = f"reduce_tasks_{job_id}"

    logger.info(f"Declaring optimized queue topology for job {job_id} (Lazy Mode Enabled)...")

    # Declare primary work queues with DLX attachments and resource caps
    await channel.declare_queue(map_queue_name, durable=True, arguments=queue_args)
    await channel.declare_queue(reduce_queue_name, durable=True, arguments=queue_args)


async def teardown_rabbitmq() -> None:
    """
    Gracefully closes active broker channels and connections.

    This function performs an orderly cleanup of messaging resources. It
    checks for open states before attempting closure to prevent unnecessary
    runtime warnings during the application shutdown phase.

    :return: None
    :rtype: None
    """
    global _global_mq_connection, _global_mq_channel

    if _global_mq_channel and not _global_mq_channel.is_closed:
        logger.info("Closing RabbitMQ channel...")
        await _global_mq_channel.close()

    if _global_mq_connection and not _global_mq_connection.is_closed:
        logger.info("Closing RabbitMQ connection...")
        await _global_mq_connection.close()

    # Reset globals to allow for clean re-initialization if needed
    _global_mq_channel = None
    _global_mq_connection = None
    logger.info("RabbitMQ lifecycle terminated.")


async def get_rabbitmq_channel() -> aio_pika.abc.AbstractRobustChannel:
    """
    FastAPI Dependency to retrieve the active RabbitMQ communication channel.

    Inject this dependency into route handlers or background services to
    publish or consume messages from the broker.

    :return: The active and initialized singleton channel.
    :rtype: aio_pika.abc.AbstractRobustChannel
    :raises RuntimeError: If the channel is accessed before `setup_rabbitmq`
        is called or if the channel has been closed.
    """
    if _global_mq_channel is None or _global_mq_channel.is_closed:
        logger.error("Attempted to access RabbitMQ channel before initialization or after closure.")
        raise RuntimeError(
            "RabbitMQ channel is not initialized or has been closed. "
            "Ensure the lifespan event is properly configured."
        )

    return _global_mq_channel