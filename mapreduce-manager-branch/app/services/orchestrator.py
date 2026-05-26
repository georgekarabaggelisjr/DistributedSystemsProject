"""
Calculation and orchestration utilities for MapReduce data partitioning.

This module provides pure utility functions to determine physical byte
boundaries, parse S3 URIs, and calculate optimal Kubernetes cluster scaling
parameters. These utilities ensure that data is partitioned consistently
and elastically across the distributed worker nodes.

The logic implements a Global Capacity Model, ensuring that resource
provisioning remains within the absolute bounds of the infrastructure
regardless of the execution phase or explicit user overrides. Furthermore,
it strictly enforces O(1) memory consumption during massive dataset
partitioning via Python Generators.
"""

import math
from urllib.parse import urlparse
from typing import Tuple, Iterator, Optional

from app.core.config import settings


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Deconstructs a standard S3 URI into its bucket and object key components.

    This function utilizes standard URL parsing to extract the network
    location (bucket) and the path (object key).

    :param uri: The full S3 URI (e.g., 's3://my-bucket/path/to/data.txt').
    :type uri: str
    :return: A two-element tuple containing (bucket_name, object_key).
    :rtype: Tuple[str, str]
    """
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip('/')


def calculate_chunks(file_size: int) -> int:
    r"""
    Determines the total number of physical Map tasks required for a given file.

    The calculation is based on the `CHUNK_SIZE_BYTES` defined in the application
    settings. The memory complexity of this operation is strictly O(1).

    The formula used is:

    $$chunks = \max\left(1, \left\lceil \frac{file\_size}{CHUNK\_SIZE\_BYTES} \right\rceil\right)$$

    :param file_size: Total size of the target file in bytes.
    :type file_size: int
    :return: The calculated number of chunks, guaranteed to be at least 1.
    :rtype: int
    """
    chunks = math.ceil(file_size / settings.CHUNK_SIZE_BYTES)
    return max(chunks, 1)


def generate_chunk_splits(file_size: int) -> Iterator[Tuple[int, int]]:
    """
    Yields byte-range boundaries for Map tasks with strict O(1) memory consumption.

    Architecture Update:
        Replaces standard list comprehension with a Python Generator. This ensures
        that processing a massive dataset (e.g., spawning 100,000 Map tasks for a
        multi-terabyte file) streams the boundaries sequentially rather than
        allocating a massive array in the Manager's local memory, preventing OOM crashes.

    :param file_size: Total size of the target file in bytes.
    :type file_size: int
    :yields: A tuple containing (byte_offset, byte_length) for a single chunk.
    :rtype: Iterator[Tuple[int, int]]
    """
    chunk_size = settings.CHUNK_SIZE_BYTES
    total_chunks = calculate_chunks(file_size)

    for i in range(total_chunks):
        offset = i * chunk_size
        # Ensure the final chunk strictly truncates at the file boundary
        length = min(chunk_size, file_size - offset)
        yield offset, length


def validate_reducers(count: int) -> Tuple[bool, str]:
    """
    Validates a user-provided reducer count against infrastructure limits.

    This function serves as the safety gate for the Hybrid Scaling Model. It
    ensures that expert overrides do not violate the Global Capacity Model
    constraints or result in a zero-partition topology.

    :param count: The number of reducers requested by the user.
    :type count: int
    :return: A tuple of (is_valid, error_reason). If valid, error_reason is empty.
    :rtype: Tuple[bool, str]
    """
    if count < 1:
        return False, "Reducer count must be at least 1."

    if count > settings.MAX_TOTAL_WORKERS:
        return False, f"Requested count ({count}) exceeds the Global Capacity limit ({settings.MAX_TOTAL_WORKERS})."

    return True, ""


def validate_mappers(count: int) -> Tuple[bool, str]:
    """
    Validates a user-provided mapper pod count against infrastructure limits.

    Ensures that an explicit override for parallel execution does not exceed
    the maximum allowed footprint of the Kubernetes cluster, preventing ResourceQuota
    rejections during provisioning.

    :param count: The number of Map worker pods requested by the user.
    :type count: int
    :return: A tuple of (is_valid, error_reason). If valid, error_reason is empty.
    :rtype: Tuple[bool, str]
    """
    if count < 1:
        return False, "Mapper count must be at least 1."

    if count > settings.MAX_TOTAL_WORKERS:
        return False, f"Requested map pods ({count}) exceeds the Global Capacity limit ({settings.MAX_TOTAL_WORKERS})."

    return True, ""


def calculate_reducers(total_chunks: int) -> int:
    r"""
    Calculates the optimal number of reduce partitions based on map scale.

    This function autonomously determines the aggregation topology by applying
    the dynamically configured Map-to-Reduce ratio.

    The logic follows the constraint:

    $$num\_reducers = \max(1, \min(\lceil total\_chunks / MAP\_TO\_REDUCE\_RATIO \rceil, MAX\_TOTAL\_WORKERS))$$

    :param total_chunks: The calculated number of Map tasks.
    :type total_chunks: int
    :return: The dynamically calculated number of reducers, capped by
        MAX_TOTAL_WORKERS and floored at 1.
    :rtype: int
    """
    map_to_reduce_ratio = settings.MAP_TO_REDUCE_RATIO
    calculated_reducers = math.ceil(total_chunks / map_to_reduce_ratio)

    return max(1, min(calculated_reducers, settings.MAX_TOTAL_WORKERS))


def get_optimal_worker_count(
    total_chunks: int,
    phase: str = "map",
    num_reducers: int = 1,
    num_mappers: Optional[int] = None
) -> int:
    """
    Calculates the target replica count for Kubernetes Batch Jobs.

    This function optimizes resource utilization by matching the number of
    worker pods to the workload size while adhering to phase-specific scaling strategies.
    Crucially, it enforces a Global Capacity Model constraint to prevent cluster exhaustion.

    Scaling Strategies:
        - Reduce Phase: 1:1 ratio. Each partition requires exactly one Reducer pod
          to execute the concurrent network shuffle and merge sort optimally.
        - Map Phase (Auto): Dynamic ratio (Task Batching). Map tasks are processed sequentially
          by a smaller pool of pods to amortize the high latency penalty of Kubernetes startup.
        - Map Phase (Manual): If `num_mappers` is provided, it bypasses the auto-scaling
          ratio and strictly honors the user's requested parallelism.

    :param total_chunks: The total number of pending tasks for the phase.
    :type total_chunks: int
    :param phase: The execution phase ('map' or 'reduce').
    :type phase: str
    :param num_reducers: The resolved partition count for the Reduce phase.
    :type num_reducers: int
    :param num_mappers: An optional explicit override for the Map pod count.
    :type num_mappers: Optional[int]
    :return: The safe, optimal number of pods to provision for the phase.
    :rtype: int
    """
    # REDUCE PHASE: Strict 1:1 mapping capped by global limits
    if phase == "reduce":
        return min(num_reducers, settings.MAX_TOTAL_WORKERS)

    # MAP PHASE: Explicit User Override
    if num_mappers is not None:
        return min(num_mappers, settings.MAX_TOTAL_WORKERS)

    # MAP PHASE: Autonomous Task Batching dynamically sourced from settings
    tasks_per_pod = settings.MAP_TASKS_PER_POD
    calculated_map_pods = math.ceil(total_chunks / tasks_per_pod)

    # Enforce the absolute cluster limit, guaranteeing we never exceed global capacity
    return min(calculated_map_pods, settings.MAX_TOTAL_WORKERS)