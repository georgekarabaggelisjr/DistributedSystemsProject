"""
Integration tests for the RabbitMQ lifecycle and Worker Messaging Fabric.

This suite validates the Infrastructure-as-Code (IaC) declarations of the
Manager's messaging layer. It ensures that queues are correctly provisioned
with durability and that the Dead Letter Exchange (DLX) is bound to prevent
infinite task retries (Poison Pill Protection).

It utilizes Docker Testcontainers to spin up ephemeral RabbitMQ instances,
ensuring the communication logic is verified against a real AMQP broker.
"""

import pytest
import pytest_asyncio
from aio_pika.abc import AbstractRobustChannel
from testcontainers.rabbitmq import RabbitMqContainer

from app.core.rabbitmq import (
    get_rabbitmq_channel,
    setup_rabbitmq,
    teardown_rabbitmq,
    declare_job_queues,
)

# --- FIXTURES ---

@pytest.fixture(scope="module")
def rabbitmq_container():
    """
    Spins up a temporary RabbitMQ Docker container for the test module.

    Provides a real AMQP environment with the management plugin enabled
    to verify queue topology and exchange bindings.

    :yields: The initialized RabbitMqContainer instance.
    :rtype: RabbitMqContainer
    """
    with RabbitMqContainer("rabbitmq:3-management") as rabbitmq:
        yield rabbitmq


@pytest_asyncio.fixture(scope="function")
async def mq_setup(rabbitmq_container, mocker):
    """
    Initializes the RabbitMQ singleton for the test session.

    This fixture patches the application settings to point to the dynamic
    port assigned by the Test container instance, ensuring zero conflict
    with local development brokers.

    :param rabbitmq_container: The active RabbitMQ container fixture.
    :type rabbitmq_container: RabbitMqContainer
    :param mocker: The pytest-mock fixture for dependency patching.
    :type mocker: pytest_mock.plugin.MockerFixture
    :yields: None
    :rtype: None
    """
    # Dynamically resolve the AMQP URL from the Test container instance
    host = rabbitmq_container.get_container_host_ip()
    port = rabbitmq_container.get_exposed_port(5672)
    amqp_url = f"amqp://guest:guest@{host}:{port}/"

    mocker.patch("app.core.rabbitmq.settings.RABBITMQ_URL", amqp_url)

    # Perform the physical declaration of exchanges and queues
    await setup_rabbitmq()
    yield
    # Graceful connection termination to prevent socket leakage
    await teardown_rabbitmq()


# --- TEST CASES ---

@pytest.mark.asyncio
async def test_rabbitmq_topology_declaration(mq_setup):
    """
    Verifies the integrity of the declared dynamic work queue topology.

    This test ensures that:
    1. Primary work queues (Map/Reduce) are declared dynamically per job.
    2. They are configured as 'Durable'.
    3. The Dead Letter Exchange (DLX) is correctly configured as an argument.
    4. The DLQ itself exists to capture failed payloads for post-mortem analysis.

    :param mq_setup: The RabbitMQ infrastructure setup fixture.
    :type mq_setup: None
    :return: None
    :rtype: None
    """
    channel = await get_rabbitmq_channel()
    test_job_id = "test-job-uuid-1234"

    # --- ARCHITECTURE FIX: Dynamically declare the queues for this specific job ---
    await declare_job_queues(channel, test_job_id)

    # 1. Verify Dynamic Work Queues exist and are configured for reliability
    for queue_name in [f"map_tasks_{test_job_id}", f"reduce_tasks_{test_job_id}"]:
        queue = await channel.get_queue(queue_name)

        # Durability ensures tasks survive a RabbitMQ broker restart
        assert queue.durable is True

        # Poison Pill Protection: Failed tasks must be routed to the DLX
        # rather than being re-queued infinitely and blocking the pipe.
        # Resolved Warning: Check for None to satisfy static analysis for __getitem__
        args = queue.arguments
        assert args is not None, f"Queue {queue_name} has no arguments defined"
        assert args["x-dead-letter-exchange"] == "dead_letter_exchange"

    # 2. Verify the global 'Graveyard' (Dead Letter Queue) is initialized
    dlq = await channel.get_queue("dead_letter_queue")
    assert dlq.durable is True


@pytest.mark.asyncio
async def test_singleton_channel_integrity(mq_setup):
    """
    Verifies the distributed singleton pattern for AMQP channels.

    Ensures that the application maintains a persistent, reusable channel
    instance across multiple calls, reducing the overhead of TCP
    handshakes and AMQP negotiation.

    :param mq_setup: The RabbitMQ infrastructure setup fixture.
    :type mq_setup: None
    :return: None
    :rtype: None
    """
    ch1: AbstractRobustChannel = await get_rabbitmq_channel()
    ch2: AbstractRobustChannel = await get_rabbitmq_channel()

    # Assert physical reference equality
    assert ch1 is ch2
    # Verify the channel is healthy and active
    assert not ch1.is_closed