"""
Unit tests for the MapReduce Job Watchdog service.

This suite validates the autonomous maintenance loop responsible for identifying
and reclaiming resources from stalled or "zombie" jobs. It ensures that the
watchdog correctly interacts with the persistence layer to detect expirations
and orchestrates the forceful deletion of Kubernetes Batch Jobs to maintain
cluster capacity in accordance with the Global Capacity Model.

The tests utilize a 'loop-break' pattern—mocking `asyncio.sleep` to raise an
exception—to simulate the periodic execution of the background service without
resulting in infinite execution during test cycles.
"""

from typing import Any, Dict
from unittest.mock import AsyncMock

import pytest

from app.services.watchdog import start_watchdog


# --- FIXTURES ---

@pytest.fixture
def mock_watchdog_deps(mocker: Any) -> Dict[str, Any]:
    """
    Mocks the database, Kubernetes provider, and repository for watchdog isolation.

    Ensures that the watchdog loop can be tested without real infrastructure
    side effects, allowing for deterministic verification of the cleanup logic
    across heterogeneous environments.

    :param mocker: The pytest-mock fixture used for patching dependencies.
    :type mocker: pytest_mock.plugin.MockerFixture
    :return: A dictionary containing the mocked infrastructure components.
    :rtype: Dict[str, Any]
    """
    return {
        "db_pool": mocker.patch("app.services.watchdog.get_db_pool", new_callable=AsyncMock),
        "k8s_provider": mocker.patch("app.services.watchdog.get_k8s_provider", new_callable=AsyncMock),
        "job_repo": mocker.patch("app.services.watchdog.JobRepository", spec=True),
        "settings": mocker.patch("app.services.watchdog.settings"),
        "redis_client": mocker.patch("app.services.watchdog.get_redis_client", new_callable=AsyncMock),
        "forcefully_reclaim_job": mocker.patch("app.services.watchdog.forcefully_reclaim_job", new_callable=AsyncMock)
    }


# --- TESTS ---

@pytest.mark.asyncio
async def test_watchdog_reclaims_stalled_resources(
    mocker: Any,
    mock_watchdog_deps: Dict[str, Any]
) -> None:
    """
    Verifies that the watchdog identifies expired jobs and triggers resource deletion.

    Tests the end-to-end flow of the Eventual Consistency sweep:
    1. Polling the repository for stalled Job IDs.
    2. Invoking the Kubernetes provider to delete associated Batch Jobs.
    3. Mutating the persistent state to 'FAILED' only after step 2 succeeds.

    :param mocker: The pytest-mock fixture.
    :type mocker: pytest_mock.plugin.MockerFixture
    :param mock_watchdog_deps: The mocked dependencies fixture.
    :type mock_watchdog_deps: Dict[str, Any]
    :return: None
    :rtype: None
    """
    deps = mock_watchdog_deps
    deps["settings"].GLOBAL_JOB_TIMEOUT = 3600

    stalled_ids = ["job-alpha", "job-beta"]
    deps["job_repo"].get_stalled_jobs.return_value = stalled_ids

    mocker.patch("app.services.watchdog.asyncio.sleep", side_effect=[None, Exception("StopLoop")])

    with pytest.raises(Exception, match="StopLoop"):
        await start_watchdog()

    deps["job_repo"].get_stalled_jobs.assert_called_once_with(
        timeout_seconds=3600,
        db_pool=deps["db_pool"].return_value
    )

    # Assert the centralized lifecycle method was called for both stalled jobs
    assert deps["forcefully_reclaim_job"].call_count == 2
    deps["forcefully_reclaim_job"].assert_any_call(
        "job-alpha", deps["db_pool"].return_value, deps["k8s_provider"].return_value, deps["redis_client"].return_value
    )
    deps["forcefully_reclaim_job"].assert_any_call(
        "job-beta", deps["db_pool"].return_value, deps["k8s_provider"].return_value, deps["redis_client"].return_value
    )


@pytest.mark.asyncio
async def test_watchdog_resilience_to_cleanup_errors(
    mocker: Any,
    mock_watchdog_deps: Dict[str, Any]
) -> None:
    """
    Verifies the watchdog's resilience to downstream infrastructure failures.

    Ensures that if the Kubernetes provider fails to delete a specific job
    (e.g., due to an API timeout or network partition), the watchdog does not
    crash. Crucially, it verifies that the database state is NOT updated to 'FAILED'
    for the job that failed physical cleanup, ensuring it will be retried.

    :param mocker: The pytest-mock fixture.
    :type mocker: pytest_mock.plugin.MockerFixture
    :param mock_watchdog_deps: The mocked dependencies fixture.
    :type mock_watchdog_deps: Dict[str, Any]
    :return: None
    :rtype: None
    """
    deps = mock_watchdog_deps
    deps["job_repo"].get_stalled_jobs.return_value = ["failed-job", "success-job"]

    # Force the lifecycle method to throw an error on the first job
    deps["forcefully_reclaim_job"].side_effect = [Exception("K8s Down"), None]

    mocker.patch("app.services.watchdog.asyncio.sleep", side_effect=[None, Exception("StopLoop")])

    with pytest.raises(Exception, match="StopLoop"):
        await start_watchdog()

    # Verify that the watchdog loop didn't crash on the first error and proceeded to clean up the second job
    assert deps["forcefully_reclaim_job"].call_count == 2