"""
Unit tests for MapReduce orchestration and scaling utilities.

This suite verifies the mathematical integrity of the partitioning logic and
the enforcement of the Global Capacity Model. It validates the
Hybrid Scaling Model, ensuring that autonomous defaults (5:1 Map ratio, 1:1 Reduce ratio)
and explicit user overrides are correctly processed and bounded by infrastructure limits.

We test for:
    - **Task Batching:** Validates that Map worker counts are amortized (1 pod per 5 tasks)
      to reduce Kubernetes control-plane pressure.
    - **Capacity Overrides:** Validates that user-provided 'num_mappers' bypasses
      auto-scaling while still adhering to the global cluster ceiling.
    - **Independent Phase Scaling:** Validates that the External Shuffle Service (ESS)
      allows phases to scale up to the absolute cluster limit independently.
"""

from typing import Any
import pytest
from app.services.orchestrator import (
    calculate_chunks,
    calculate_reducers,
    get_optimal_worker_count,
    parse_s3_uri,
    validate_reducers,
    validate_mappers,
)


def test_parse_s3_uri_standard() -> None:
    """
    Verifies the deconstruction of standard S3 URIs into bucket and key components.
    """
    uri = "s3://integration-test-bucket/large_data.txt"
    bucket, key = parse_s3_uri(uri)

    assert bucket == "integration-test-bucket"
    assert key == "large_data.txt"


def test_calculate_chunks_scaling(mocker: Any) -> None:
    """
    Verifies physical data partitioning based on configured CHUNK_SIZE_BYTES.
    """
    chunk_size = 64 * 1024 * 1024
    mocker.patch("app.services.orchestrator.settings.CHUNK_SIZE_BYTES", chunk_size)

    # Exactly one chunk boundary
    assert calculate_chunks(chunk_size) == 1

    # Slight overflow results in an additional chunk (64MB + 1KB)
    assert calculate_chunks(chunk_size + 1024) == 2

    # Minimum floor of 1 for tiny files
    assert calculate_chunks(100) == 1


def test_validate_reducers_boundaries(mocker: Any) -> None:
    """
    Verifies the safety gate for user-defined reducer counts against global limits.
    """
    mocker.patch("app.services.orchestrator.settings.MAX_TOTAL_WORKERS", 15)

    assert validate_reducers(1)[0] is True
    assert validate_reducers(15)[0] is True

    # Invalid floor
    valid, reason = validate_reducers(0)
    assert valid is False
    assert "at least 1" in reason

    # Invalid ceiling
    valid, reason = validate_reducers(16)
    assert valid is False
    assert "exceeds the Global Capacity limit" in reason


def test_validate_mappers_boundaries(mocker: Any) -> None:
    """
    Verifies the safety gate for user-defined mapper overrides.
    """
    mocker.patch("app.services.orchestrator.settings.MAX_TOTAL_WORKERS", 20)

    assert validate_mappers(1)[0] is True
    assert validate_mappers(20)[0] is True

    # Invalid ceiling
    valid, reason = validate_mappers(21)
    assert valid is False
    assert "exceeds the Global Capacity limit" in reason


def test_calculate_reducers_auto_scaling(mocker: Any) -> None:
    """
    Verifies the autonomous 3:1 Map-to-Reduce scaling ratio.
    """
    mocker.patch("app.services.orchestrator.settings.MAX_TOTAL_WORKERS", 15)

    # 1 map chunk -> 1 reducer (floor)
    assert calculate_reducers(1) == 1

    # 6 map chunks -> 2 reducers (3:1 ratio)
    assert calculate_reducers(6) == 2

    # 4 map chunks -> 2 reducers (ceiling of partial ratio)
    assert calculate_reducers(4) == 2

    # Capped at global limit
    assert calculate_reducers(100) == 15


def test_get_optimal_worker_count_logic(mocker: Any) -> None:
    """
    Verifies pod replica counts with Task Batching and User Overrides.
    """
    mocker.patch("app.services.orchestrator.settings.MAX_TOTAL_WORKERS", 10)

    # --- MAP PHASE: Auto-Scaling (5 tasks per pod) ---

    # Case 1: 5 chunks / 5 ratio = 1 Pod
    assert get_optimal_worker_count(5, phase="map") == 1

    # Case 2: 25 chunks / 5 ratio = 5 Pods
    assert get_optimal_worker_count(25, phase="map") == 5

    # Case 3: 100 chunks / 5 ratio = 20 Pods -> Capped at MAX_TOTAL_WORKERS (10)
    assert get_optimal_worker_count(100, phase="map") == 10

    # --- MAP PHASE: User Override (num_mappers) ---

    # User requests exactly 7 pods; bypasses batching ratio
    assert get_optimal_worker_count(100, phase="map", num_mappers=7) == 7

    # User requests 50 pods; capped at global limit
    assert get_optimal_worker_count(100, phase="map", num_mappers=50) == 10

    # --- REDUCE PHASE: 1:1 Scaling ---

    # Reducers always map 1 pod to 1 partition
    assert get_optimal_worker_count(0, phase="reduce", num_reducers=4) == 4

    # Capped at global limit
    assert get_optimal_worker_count(0, phase="reduce", num_reducers=15) == 10


@pytest.mark.parametrize("file_size, expected_chunks", [
    (1024, 1),             # Scenario: Tiny file (< CHUNK_SIZE)
    (67108864, 1),         # Scenario: Exactly 64MB (1 Chunk)
    (67108865, 2),         # Scenario: 64MB + 1 byte (Overflow to 2 Chunks)
    (201326592, 3),        # Scenario: Exactly 192MB (64MB * 3)
])
def test_partitioning_boundaries(
    mocker: Any,
    file_size: int,
    expected_chunks: int
) -> None:
    """
    Performs boundary testing for physical chunking using parametrization.
    """
    mocker.patch("app.services.orchestrator.settings.CHUNK_SIZE_BYTES", 64 * 1024 * 1024)
    assert calculate_chunks(file_size) == expected_chunks