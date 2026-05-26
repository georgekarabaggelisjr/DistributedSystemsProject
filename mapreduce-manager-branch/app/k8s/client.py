"""
Kubernetes cluster orchestration module.

This module provides a high-level abstraction layer for managing distributed worker
resources within a Kubernetes cluster. It leverages the `kubernetes-asyncio`
library for non-blocking API interactions and implements a Strategy pattern
via the `KubernetesProvider` interface to support seamless local mocking.

The module is responsible for:
    * Dynamic manifest generation via Jinja2 templates.
    * Secure environment variable injection for RabbitMQ and MinIO credentials.
    * Monitoring distributed job lifecycles via stateless polling barriers.
    * Enforcing Data Locality through Kubernetes Node Affinity (NodeSelectors).
    * Dynamic resource cleanup (Kubernetes Jobs and AMQP Queues).
"""

import asyncio
import logging
import random
import string
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import aiofiles
import yaml
from jinja2 import Template, TemplateError
from kubernetes_asyncio import client, config
from kubernetes_asyncio.client.exceptions import ApiException

from app.core.config import settings
from app.core.rabbitmq import get_rabbitmq_channel

# Initialize the module-level logger for cluster events
logger = logging.getLogger(__name__)


class KubernetesProvider(ABC):
    """
    Abstract base class defining the contract for Kubernetes resource providers.

    Defines the essential lifecycle operations for managing worker pods across
    different deployment environments (e.g., local, cloud, or mock).
    """

    @abstractmethod
    async def spawn_workers(
        self,
        job_id: str,
        replicas: int,
        phase: str = "map",
        partition_id: Optional[int] = None,
        target_node: Optional[str] = None
    ) -> None:
        """
        Provision worker pods for a specific execution phase.

        Args:
            job_id: The unique identifier for the MapReduce job.
            replicas: The number of worker units requested.
            phase: The current execution phase (e.g., 'map' or 'reduce').
            partition_id: Optional identifier used to name individual Reducer micro-jobs.
            target_node: Physical node hostname hint for Data Locality Affinity.
        """
        pass

    @abstractmethod
    async def wait_for_job_completion(self, job_id: str) -> None:
        """
        Block asynchronously until all workers for a specific job have finished.

        Args:
            job_id: The job identifier to monitor.
        """
        pass

    @abstractmethod
    async def delete_job(self, job_id: str) -> None:
        """
        Clean up Kubernetes resources and message queues associated with a specific job.

        Args:
            job_id: The job identifier whose resources should be purged.
        """
        pass


class RealKubernetesProvider(KubernetesProvider):
    """
    Production-grade Kubernetes provider communicating with the Batch V1 API.

    This implementation handles the physical creation and monitoring of
    Namespaced Batch Jobs, using the official Kubernetes Python client
    in asynchronous mode.
    """

    def __init__(self):
        # In-memory cache to prevent disk I/O bottlenecks during massive concurrent pod spawns
        self._worker_template_cache: Optional[str] = None

    async def spawn_workers(
            self,
            job_id: str,
            replicas: int,
            phase: str = "map",
            partition_id: Optional[int] = None,
            target_node: Optional[str] = None
    ) -> None:
        """
        Renders a YAML manifest and submits a Batch Job to the Kubernetes API.

        Implements a Lazy Loading Memory Cache for the worker YAML template.
        When provisioning dozens of individual Reducer micro-jobs concurrently
        for Data Locality, this eliminates identical overlapping disk reads,
        significantly reducing latency and OS-level I/O contention.
        """
        # Ensure the cluster is not overwhelmed by capping worker counts
        actual_replicas = min(replicas, settings.MAX_TOTAL_WORKERS)

        if replicas > actual_replicas:
            logger.info("Requested %d %s workers, capping at MAX_TOTAL_WORKERS.", replicas, phase)

        node_msg = f" pinned to {target_node}" if target_node else ""
        logger.info("Requesting creation of %d %s worker pods%s (Guaranteed QoS Tier).", actual_replicas, phase,
                    node_msg)

        # Generate unique suffix to avoid job name collisions in K8s
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=5))
        safe_job_id = str(job_id)[:8]

        # Use partition IDs to facilitate single-replica Data Locality jobs
        if partition_id is not None:
            k8s_job_name = f"worker-{phase}-{safe_job_id}-p{partition_id}-{random_suffix}"
        else:
            k8s_job_name = f"worker-{phase}-{safe_job_id}-{random_suffix}"

        try:
            # --- CACHE CHECK (O(1) Memory Access) ---
            if self._worker_template_cache is None:
                template_path = Path(__file__).parent / "templates" / "worker-template.yaml"
                async with aiofiles.open(template_path, "r") as file:
                    self._worker_template_cache = await file.read()

            template_str = self._worker_template_cache

            # Resolve internal networking for Docker/Kubernetes environments
            mq_host = urlparse(settings.RABBITMQ_URL).hostname or "rabbitmq"
            minio_endpoint = settings.MINIO_ENDPOINT

            if mq_host in ("localhost", "127.0.0.1"):
                mq_host = "host.docker.internal"
            if minio_endpoint.startswith("localhost") or minio_endpoint.startswith("127.0.0.1"):
                minio_endpoint = minio_endpoint.replace("localhost", "host.docker.internal").replace("127.0.0.1",
                                                                                                     "host.docker.internal")

            # Render manifest using Jinja2 for dynamic configuration injection
            jinja_template = Template(template_str) # type: ignore
            manifest_str = jinja_template.render(
                JOB_NAME=k8s_job_name,
                REPLICAS=actual_replicas,
                JOB_ID=str(job_id),
                PHASE=phase,
                K8S_NAMESPACE=settings.K8S_NAMESPACE,
                RABBITMQ_HOST=mq_host,
                MINIO_ENDPOINT=minio_endpoint,
                MINIO_ACCESS_KEY=settings.MINIO_ACCESS_KEY,
                MINIO_SECRET_KEY=settings.MINIO_SECRET_KEY,
                TARGET_NODE=target_node  # Locality hint used for NodeSelector affinity
            )

            job_dict: Dict[str, Any] = yaml.safe_load(manifest_str)

            async with client.ApiClient() as api_client:
                batch_v1 = client.BatchV1Api(api_client)

                await batch_v1.create_namespaced_job(
                    namespace=settings.K8S_NAMESPACE,
                    body=job_dict  # type: ignore[arg-type]
                )
            logger.info("Successfully submitted K8s Job: %s", k8s_job_name)

        except ApiException as e:
            logger.error("Kubernetes API rejected manifest for %s: %s", k8s_job_name, e.body)
            raise
        except (FileNotFoundError, yaml.YAMLError, TemplateError) as e:
            logger.error("Configuration or Template error in %s worker spawn: %s", phase, str(e))
            raise RuntimeError(f"Failed to render or load worker manifest: {e}") from e
        except Exception as e:
            logger.error("Unexpected error spawning workers: %s", str(e))
            raise

    async def wait_for_job_completion(self, job_id: str) -> None:
        """
        Polls the Kubernetes Batch API until all Job completions are reported.

        Args:
            job_id: The job ID used in the label selector.

        Raises:
            Exception: If an unrecoverable error occurs during polling.
        """
        safe_job_id = str(job_id)[:8]
        logger.info("Starting stateless API polling for K8s Job completion (ID: %s)...", safe_job_id)

        label_selector = f"job_id={job_id}"
        timeout_seconds = settings.GLOBAL_JOB_TIMEOUT
        poll_interval = 5
        elapsed = 0

        async with client.ApiClient() as api_client:
            batch_v1 = client.BatchV1Api(api_client)

            while elapsed < timeout_seconds:
                try:
                    job_list = await batch_v1.list_namespaced_job(
                        namespace=settings.K8S_NAMESPACE,
                        label_selector=label_selector
                    )

                    if not job_list.items:
                        logger.debug("No active K8s Jobs found for %s yet.", safe_job_id)
                    else:
                        all_succeeded = True
                        for k8s_job in job_list.items:
                            expected_completions = k8s_job.spec.completions or 1
                            current_successes = k8s_job.status.succeeded or 0

                            if current_successes < expected_completions:
                                all_succeeded = False
                                break

                        if all_succeeded:
                            logger.info("All worker pods for job %s exited successfully.", safe_job_id)
                            return

                except ApiException as e:
                    logger.warning("Transient error polling K8s job status: %s", e.body)
                except Exception as e:
                    logger.error("Unexpected error during K8s polling: %s", str(e))
                    raise

                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            logger.error("Timeout reached for job %s after %ss.", safe_job_id, timeout_seconds)

    async def delete_job(self, job_id: str) -> None:
        """
        Purges all Kubernetes Job resources and dynamic message queues
        identified by the job_id label.

        Args:
            job_id: The job ID used to find resources for deletion.
        """
        logger.info("Initiating resource cleanup for finished job.")
        # Brief sleep ensures log aggregation completes before resource removal
        await asyncio.sleep(5)

        safe_job_id = str(job_id)[:8]

        # Clean up dynamic RabbitMQ queues associated with the job
        try:
            mq_channel = await get_rabbitmq_channel()
            map_queue = f"map_tasks_{job_id}"
            reduce_queue = f"reduce_tasks_{job_id}"

            # Delete queues if they exist (if_unused/if_empty flags can be added if needed)
            await mq_channel.queue_delete(map_queue)
            await mq_channel.queue_delete(reduce_queue)
            logger.info("Successfully deleted dynamic RabbitMQ queues for job %s.", safe_job_id)
        except Exception as e:
            logger.error("Failed to delete RabbitMQ queues for job %s: %s", safe_job_id, str(e))
        # ---------------------------------------------------------

        async with client.ApiClient() as api_client:
            batch_v1 = client.BatchV1Api(api_client)

            try:
                job_list = await batch_v1.list_namespaced_job(namespace=settings.K8S_NAMESPACE)

                for k8s_job in job_list.items:
                    if k8s_job.metadata.name and safe_job_id in k8s_job.metadata.name:
                        # Use Background propagation to ensure Pods are also cleaned up
                        await batch_v1.delete_namespaced_job(
                            name=k8s_job.metadata.name,
                            namespace=settings.K8S_NAMESPACE,
                            propagation_policy="Background"
                        )
                        logger.info("Deleted K8s Job resource: %s", k8s_job.metadata.name)

            except ApiException as e:
                logger.error("Failed to clean up Kubernetes resources for job %s: %s", job_id, e.body)


class MockKubernetesProvider(KubernetesProvider):
    """
    Simulation provider for offline development and local unit testing.

    Mocks all Kubernetes API interactions while maintaining standard logging
    and asynchronous delays to mimic real cluster behavior.
    """

    async def spawn_workers(
        self,
        job_id: str,
        replicas: int,
        phase: str = "map",
        partition_id: Optional[int] = None,
        target_node: Optional[str] = None
    ) -> None:
        """Logs the simulation of worker pod creation."""
        actual_replicas = min(replicas, settings.MAX_TOTAL_WORKERS)
        part_info = f" (Partition {partition_id})" if partition_id is not None else ""
        node_info = f" [Pinned to: {target_node}]" if target_node else ""
        logger.info("[MOCK] Spawning %d %s workers for job %s%s%s", actual_replicas, phase, job_id, part_info, node_info)

    async def wait_for_job_completion(self, job_id: str) -> None:
        """Simulates an asynchronous wait period for job completion."""
        logger.info("[MOCK] Waiting for job %s completion...", job_id)
        await asyncio.sleep(1)

    async def delete_job(self, job_id: str) -> None:
        """Logs the simulation of resource and queue cleanup."""
        logger.info("[MOCK] Cleaning up K8s resources and MQ queues for job %s", job_id)


# Global singleton instance holder for the provider
_global_k8s_provider: Optional[KubernetesProvider] = None


async def setup_k8s() -> None:
    """
    Analyzes the environment and initializes the appropriate Kubernetes provider.

    Discovery sequence:
        1. Checks if MOCK_K8S setting is active.
        2. Attempts to load in-cluster configuration (ServiceAccount).
        3. Attempts to load local kubeconfig (Dev environment).
        4. Falls back to Mock if no cluster is reachable.
    """
    global _global_k8s_provider

    if settings.MOCK_K8S:
        logger.info("Initializing MockKubernetesProvider (MOCK_K8S=True)")
        _global_k8s_provider = MockKubernetesProvider()
        return

    try:
        config.load_incluster_config()
        logger.info("Detected In-Cluster configuration. Using RealKubernetesProvider.")
        _global_k8s_provider = RealKubernetesProvider()
    except (config.ConfigException, AttributeError):
        try:
            await config.load_kube_config()
            logger.info("Detected Local Kubeconfig. Using RealKubernetesProvider.")
            _global_k8s_provider = RealKubernetesProvider()
        except (config.ConfigException, OSError):
            logger.warning("No cluster connectivity detected. Falling back to Mock.")
            _global_k8s_provider = MockKubernetesProvider()


async def get_k8s_provider() -> KubernetesProvider:
    """
    Retrieves the active Kubernetes provider instance.

    Returns:
        KubernetesProvider: The initialized global provider.

    Raises:
        RuntimeError: If setup_k8s() has not been called prior to this request.
    """
    if _global_k8s_provider is None:
        raise RuntimeError("Kubernetes provider is not initialized.")
    return _global_k8s_provider

async def get_active_pod_count(job_id: str) -> int:
    """
    Queries the Kubernetes API for the live count of pods belonging to a job.
    """
    label_selector = f"job_id={job_id}"

    async with client.ApiClient() as api_client:
        # We use CoreV1Api because Pods are a Core resource, not Batch
        core_v1 = client.CoreV1Api(api_client)

        pods = await core_v1.list_namespaced_pod(
            namespace=settings.K8S_NAMESPACE,
            label_selector=label_selector
        )

        # Active phases = running or about to run
        active_phases = ["Running", "Pending"]
        return len([p for p in pods.items if p.status.phase in active_phases])