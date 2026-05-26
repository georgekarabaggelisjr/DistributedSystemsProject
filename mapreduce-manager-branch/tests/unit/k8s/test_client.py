"""
Unit tests for the Kubernetes infrastructure orchestration client.

This suite provides comprehensive validation for the lifecycle management of worker
pods via the Kubernetes Batch V1 API. It ensures that the Jinja2 template engine
correctly hydrates manifests, node selectors are applied securely, and the
asynchronous barrier logic handles job completion deterministically.

These tests verify the Data Locality Scheduling requirements, ensuring Reducer
pods are correctly pinned to physical hostnames and that the physical identity
(NODE_IP and NODE_NAME) is propagated via the template's Downward API configurations.
"""

from typing import Any, Dict
from unittest.mock import MagicMock, AsyncMock

import pytest
from app.k8s.client import MockKubernetesProvider, RealKubernetesProvider


# --- FIXTURES ---

@pytest.fixture
def mock_settings(mocker: Any) -> None:
    """
    Mocks global infrastructure settings to ensure predictable test conditions.

    :param mocker: The pytest-mock fixture.
    """
    mocker.patch("app.k8s.client.settings.MAX_TOTAL_WORKERS", 5)
    mocker.patch("app.k8s.client.settings.K8S_NAMESPACE", "test-namespace")


@pytest.fixture
def dummy_worker_template() -> str:
    """
    Provides a minimal valid Jinja2 template for testing manifest rendering.

    Includes a conditional nodeSelector to support Data Locality tests, and
    simulates the presence of Downward API configurations.

    :return: A string representation of a Kubernetes Job YAML template.
    """
    return """
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ JOB_NAME }}
spec:
  completions: {{ REPLICAS }}
  template:
    spec:
      {% if TARGET_NODE %}
      nodeSelector:
        kubernetes.io/hostname: "{{ TARGET_NODE }}"
      {% endif %}
      containers:
      - name: worker
        env:
        - name: EXISTING_VAR
          value: "keep_me"
        - name: IDLE_TIMEOUT_MILLIS
          value: "30000"
        - name: NODE_IP
          valueFrom:
            fieldRef:
              fieldPath: status.hostIP
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
"""


@pytest.fixture
def mock_k8s_api(mocker: Any) -> Dict[str, MagicMock]:
    """
    Standardizes the mocking of the Kubernetes ApiClient and BatchV1Api.

    Ensures 'deserialize' is synchronous to avoid coroutine subscription errors
    common in async Kubernetes clients.

    :param mocker: The pytest-mock fixture.
    :return: A dictionary of mocked API components.
    """
    mock_api_client_cls = mocker.patch("app.k8s.client.client.ApiClient")
    api_instance = mock_api_client_cls.return_value

    api_instance.__aenter__ = AsyncMock(return_value=api_instance)
    api_instance.__aexit__ = AsyncMock(return_value=None)
    api_instance.deserialize = MagicMock(side_effect=lambda _model, data, _type: data)

    mock_batch_cls = mocker.patch("app.k8s.client.client.BatchV1Api")
    mock_create_job = AsyncMock()
    mock_batch_cls.return_value.create_namespaced_job = mock_create_job
    mock_list_jobs = AsyncMock()
    mock_batch_cls.return_value.list_namespaced_job = mock_list_jobs

    return {
        "api_instance": api_instance,
        "create_job": mock_create_job,
        "list_jobs": mock_list_jobs
    }


# --- TESTS ---

@pytest.mark.asyncio
async def test_mock_provider_execution() -> None:
    """
    Verifies the operational safety and API parity of the MockKubernetesProvider.
    """
    provider = MockKubernetesProvider()
    await provider.spawn_workers("job-123", 10)
    await provider.wait_for_job_completion("job-123")
    await provider.delete_job("job-123")


@pytest.mark.asyncio
async def test_spawn_workers_renders_and_submits(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
    dummy_worker_template: str
) -> None:
    """
    Validates the 'Map' phase manifest generation and API submission sequence.

    Verifies that environment variables defined in the template (such as
    NODE_IP and NODE_NAME) are correctly preserved and sent to the K8s API.
    """
    mock_file = AsyncMock()
    mock_file.read.return_value = dummy_worker_template
    mock_open = mocker.patch("app.k8s.client.aiofiles.open")
    mock_open.return_value.__aenter__.return_value = mock_file

    provider = RealKubernetesProvider()
    await provider.spawn_workers(job_id="test-job-uuid", replicas=10, phase="map")

    create_job_mock = mock_k8s_api["create_job"]
    create_job_mock.assert_called_once()

    call_args = create_job_mock.call_args
    assert call_args is not None
    submitted_manifest: Dict[str, Any] = call_args.kwargs["body"]

    # Verify basic metadata and constraints
    assert call_args.kwargs["namespace"] == "test-namespace"
    assert "worker-map-test-job" in submitted_manifest["metadata"]["name"]
    assert submitted_manifest["spec"]["completions"] == 5  # Capped by mock_settings

    # Verify Environment Variables present in the template manifest
    env_vars = submitted_manifest["spec"]["template"]["spec"]["containers"][0]["env"]

    # Helper to retrieve an env var definition safely
    def get_env(name: str) -> Dict[str, Any]:
        return next((e for e in env_vars if e["name"] == name), {})

    assert get_env("EXISTING_VAR") == {"name": "EXISTING_VAR", "value": "keep_me"}
    assert get_env("IDLE_TIMEOUT_MILLIS") == {"name": "IDLE_TIMEOUT_MILLIS", "value": "30000"}

    # VITAL: Verify Downward API Identity properties are preserved
    node_ip_env = get_env("NODE_IP")
    assert node_ip_env.get("valueFrom", {}).get("fieldRef", {}).get("fieldPath") == "status.hostIP"

    node_name_env = get_env("NODE_NAME")
    assert node_name_env.get("valueFrom", {}).get("fieldRef", {}).get("fieldPath") == "spec.nodeName"


@pytest.mark.asyncio
async def test_spawn_workers_with_data_locality(
    mocker: Any,
    mock_settings: None,
    mock_k8s_api: Dict[str, Any],
    dummy_worker_template: str
) -> None:
    """
    Verifies that the provider applies targeted node affinity when a
    physical host hint is provided by the Manager.
    """
    mock_file = AsyncMock()
    mock_file.read.return_value = dummy_worker_template
    mocker.patch("app.k8s.client.aiofiles.open").return_value.__aenter__.return_value = mock_file

    provider = RealKubernetesProvider()
    target_host = "physical-node-01"

    await provider.spawn_workers(
        job_id="locality-test",
        replicas=1,
        phase="reduce",
        target_node=target_host
    )

    call_args = mock_k8s_api["create_job"].call_args
    submitted_manifest = call_args.kwargs["body"]

    # Assert the Reducer pod is explicitly pinned to the specific node
    node_selector = submitted_manifest["spec"]["template"]["spec"]["nodeSelector"]
    assert node_selector["kubernetes.io/hostname"] == target_host


@pytest.mark.asyncio
async def test_wait_for_job_completion_polling_logic(
    mocker: Any,
    mock_k8s_api: Dict[str, Any]
) -> None:
    """
    Tests the stateless polling barrier logic using the Kubernetes API.
    """
    mock_job_unfinished = MagicMock()
    mock_job_unfinished.spec.completions = 3
    mock_job_unfinished.status.succeeded = 1

    mock_job_finished = MagicMock()
    mock_job_finished.spec.completions = 3
    mock_job_finished.status.succeeded = 3

    mock_list_unfinished = MagicMock(items=[mock_job_unfinished])
    mock_list_finished = MagicMock(items=[mock_job_finished])

    # Configure the list_jobs mock to simulate progressive completion
    mock_k8s_api["list_jobs"].side_effect = [mock_list_unfinished, mock_list_finished]

    # Patch sleep to accelerate test execution
    mocker.patch("app.k8s.client.asyncio.sleep", new_callable=AsyncMock)

    provider = RealKubernetesProvider()
    await provider.wait_for_job_completion(job_id="poll-test")

    assert mock_k8s_api["list_jobs"].call_count == 2