"""
Service module for object storage operations.

This module abstracts interactions with MinIO/S3 compatible storage layers,
providing high-level utilities for secure client initialization and
multi-tenant file resolution.

This service operates strictly as a data access layer, as execution
classes are derived dynamically by the Orchestrator directly from validated
filenames. This ensures a leaner, faster, and more secure storage boundary.
"""

import logging
from minio import Minio
from app.core.config import settings

# Initialize module-level logger for storage events
logger = logging.getLogger(__name__)

class StorageService:
    """
    Encapsulates S3/MinIO operations and client initialization.

    This service provides a centralized interface for interacting with
    S3-compatible storage. It handles the low-level MinIO client configuration
    necessary for the Orchestrator to resolve physical file sizes, calculate
    byte offsets, and manage sandboxed multi-tenant execution paths.
    """

    @staticmethod
    def get_client() -> Minio:
        """
        Initializes and returns a configured MinIO client.

        Uses the global application settings for endpoint, credentials,
        and security protocols to establish a secure connection to the
        storage cluster.

        Returns:
            Minio: An authenticated MinIO client instance ready for I/O operations.
        """
        return Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )