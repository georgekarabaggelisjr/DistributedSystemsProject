"""
Application configuration module.

This module leverages Pydantic's :code:`BaseSettings` to provide robust environment
variable management, type validation, and centralized static configuration.
Values defined here can be overridden by system environment variables or a
local :code:`.env` file for development flexibility.

Usage:
    .. code-block:: python

        from app.core.config import settings
        print(settings.DATABASE_URL)
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Global application settings and environment configuration.

    This class defines the schema for the application's configuration. Pydantic
    automatically maps environment variables to these attributes. If an environment
    variable is present (e.g., DATABASE_URL), it takes precedence over the default
    values defined below.

    Attributes:
        MAX_TOTAL_WORKERS (int): The upper bound for parallel worker pods
            the orchestrator can provision at any given time.
        CHUNK_SIZE_BYTES (int): The target size for data partitions. 128MB is
            chosen to balance network throughput and task granularity.
        GLOBAL_JOB_TIMEOUT (int): The watchdog timer threshold. Jobs exceeding
            this duration are marked for forceful reclamation.
        IDEMPOTENCY_LOCK_TTL (int): Short-lived lock duration (seconds) used
            during the active orchestration phase to prevent permanent lockouts
            on manager crashes.
        IDEMPOTENCY_CACHE_TTL (int): Long-lived cache duration (seconds) for
            persisting successful job scheduling responses.
        DATABASE_URL (str): SQLAlchemy-compatible connection string for PostgreSQL.
        RABBITMQ_URL (str): AMQP connection string for the message broker.
        REDIS_URL (str): Connection string for the in-memory distributed cache.
        MINIO_ENDPOINT (str): The host and port for the S3-compatible storage service.
        MINIO_ACCESS_KEY (str): Credentials for MinIO/S3 access.
        MINIO_SECRET_KEY (str): Credentials for MinIO/S3 secret access.
        MINIO_SECURE (bool): Enable/disable HTTPS for storage communication.
        MOCK_K8S (bool): Toggle for local development without a live K8s cluster.
        K8S_NAMESPACE (str): The target K8s namespace for worker deployments.
        MAP_TO_REDUCE_RATIO (int): The default Map-to-Reduce partition ratio
            (e.g., 3 means 3 Map tasks yield 1 Reducer).
        MAP_TASKS_PER_POD (int): The default task batching size for Map pods
            to amortize Kubernetes boot latency.
        JWT_PUBLIC_KEY (str): The public key used to verify the RSA signature
            of incoming JWT tokens (e.g., from the Keycloak realm).
        JWT_ALGORITHM (str): The cryptographic algorithm used for JWT signing.
        JWT_AUDIENCE (str): The intended audience claim required in the JWT payload.
    """

    # --- Orchestration Constraints ---
    MAX_TOTAL_WORKERS: int = 50
    CHUNK_SIZE_BYTES: int = 128 * 1024 * 1024
    GLOBAL_JOB_TIMEOUT: int = 3600

    # --- Scaling & Topology Ratios ---
    # The default Map-to-Reduce partition ratio (3 Map tasks yield 1 Reducer)
    MAP_TO_REDUCE_RATIO: int = 3
    # The default task batching size for Map pods (5 Tasks per Pod) to amortize boot latency
    MAP_TASKS_PER_POD: int = 5

    # --- Idempotency Strategy (Fix #2) ---
    # 60s protects against "zombie" locks if the manager service crashes.
    IDEMPOTENCY_LOCK_TTL: int = 60
    # 24h ensures clients receive the same response for repeated retries.
    IDEMPOTENCY_CACHE_TTL: int = 86400

    # --- Core Infrastructure Connections ---
    DATABASE_URL: str = "postgresql://postgres:DisSami2026!@localhost:5432/dds_db"
    RABBITMQ_URL: str = "amqp://guest:guest@localhost:5672/"
    REDIS_URL: str = "redis://localhost:6379/0"

    # --- MinIO / S3 Storage Settings ---
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False

    # --- Kubernetes Deployment Settings ---
    MOCK_K8S: bool = False
    K8S_NAMESPACE: str = "default"

    # --- Authentication & Security ---
    ENABLE_AUTH: bool = False
    """Toggle to globally enforce or bypass token authorization gates during testing profiles."""

    JWT_PUBLIC_KEY: str = "your-keycloak-realm-public-key-here"
    JWT_ALGORITHM: str = "RS256"
    JWT_AUDIENCE: str = "account"

    class Config:
        """Pydantic configuration for the Settings class."""
        env_file = ".env"
        case_sensitive = True


# Global settings singleton used for dependency injection throughout the API
settings = Settings()