"""
Main FastAPI application entry point.

This module initializes the FastAPI application and manages the global
lifespan of the system's infrastructure. It acts as the central coordinator
for the application's boot sequence, handling the transition from a cold
start to a fully operational state where database pools, message brokers,
and background workers are active.

The orchestration follows a "Safety First" pattern:
1. Initialize core logging and connectivity.
2. Launch autonomous background services (Consumer, Watchdog, Reconciler).
3. Yield control to the FastAPI router to begin serving traffic.
4. Gracefully reclaim all resources and drain pools on shutdown.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router as api_router
from app.core.database import setup_database, teardown_database
from app.core.logger import setup_logging
from app.core.rabbitmq import setup_rabbitmq, teardown_rabbitmq
from app.core.redis import setup_redis, teardown_redis
from app.k8s.client import setup_k8s
from app.services.task_consumer import start_consumer
from app.services.watchdog import start_watchdog
from app.services.reconciler import reconciler_loop

# Initialize module-level logger for application lifecycle events
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    """
    Manages the global application lifecycle and infrastructure dependencies.

    This asynchronous context manager handles the sequential initialization
    of external services (PostgreSQL, RabbitMQ, Redis, and Kubernetes) before
    the API starts accepting traffic.

    During the startup phase, it launches three primary background services
    as non-blocking asyncio tasks:
        * **Task Consumer**: Listens for worker completion/failure signals.
        * **Watchdog**: Periodically reclaims resources from stalled jobs.
        * **Reconciler**: Audits and synchronizes Redis capacity reservations
          against the actual Kubernetes pod state to prevent state drift.

    During the shutdown phase, it ensures that these background tasks are
    gracefully canceled and all connection pools are drained to prevent
    hanging sockets or zombie processes.

    :param _: The running FastAPI instance.
    :type _: FastAPI
    :yields: Control is yielded back to the FastAPI framework to begin
             serving HTTP requests.
    """
    # --- STARTUP PHASE ---
    # Logging must be first to capture initialization logs from other layers
    setup_logging()
    logger.info("Starting MapReduce Manager API initialization...")

    # Initialize task placeholders to safely handle shutdown if startup fails
    consumer_task = None
    watchdog_task = None
    reconciler_task = None

    try:
        # Step 1: Establish persistent connections to core state and messaging
        await setup_database()
        await setup_redis()
        await setup_rabbitmq()
        await setup_k8s()

        logger.info("Infrastructure layers (DB, Cache, MQ, K8s) connected.")

        # Step 2: Launch background tasks as non-blocking asyncio Tasks.
        # These operate independently of the HTTP request/response cycle.
        consumer_task = asyncio.create_task(start_consumer())
        watchdog_task = asyncio.create_task(start_watchdog())
        reconciler_task = asyncio.create_task(reconciler_loop())

        logger.info("Background services (Consumer, Watchdog, Reconciler) activated.")

        # Transition to operational state
        yield

    finally:
        # --- SHUTDOWN PHASE ---
        # Triggered when the process receives a termination signal (SIGTERM/SIGINT)
        # or if an exception occurs during the startup phase.
        logger.info("Shutdown initiated. Terminating background services...")

        tasks_to_await = []

        # Explicitly stop background loops only if they were successfully created
        if consumer_task:
            consumer_task.cancel()
            tasks_to_await.append(consumer_task)

        if watchdog_task:
            watchdog_task.cancel()
            tasks_to_await.append(watchdog_task)

        if reconciler_task:
            reconciler_task.cancel()
            tasks_to_await.append(reconciler_task)

        if tasks_to_await:
            try:
                # Await task cancellation, allowing them to finish current work.
                # return_exceptions=True prevents a CancelledError from stopping the sequence.
                await asyncio.gather(*tasks_to_await, return_exceptions=True)
            except Exception as e:
                logger.error("Error during background task shutdown: %s", str(e))

        # Step 3: Cleanly close infrastructure connections
        await teardown_rabbitmq()
        await teardown_redis()
        await teardown_database()
        logger.info("MapReduce Manager API gracefully shut down.")


# Initialize the FastAPI instance with metadata for automated documentation generators.
# These fields populate the 'Info' section of the Swagger (HTML) documentation.
app = FastAPI(
    title="MapReduce Manager API",
    description=(
        "Distributed orchestration service responsible for job scheduling, "
        "partition calculation, and infrastructure scaling."
    ),
    version="1.0.0",
    lifespan=lifespan
)

# Register the internal orchestration and scheduling routes.
# This attaches the /internal/schedule and related endpoints to the root app.
app.include_router(api_router)