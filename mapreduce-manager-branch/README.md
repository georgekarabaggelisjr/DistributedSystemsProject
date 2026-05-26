# MapReduce Orchestrator (Manager Service)

The **MapReduce Orchestrator** is the central control plane for a distributed, Kubernetes-native MapReduce framework. Designed for cloud-native elasticity and high-throughput environments, the Manager handles job scheduling, multi-tenant sandboxing, O(1) memory data partitioning, and autonomous fault recovery across volatile compute clusters.

---

## 🏗 Architecture Overview

The Manager operates on an **Event-Driven, Active-Active Architecture**, decoupling HTTP API ingestion from asynchronous worker execution. System state is managed safely across three distinct persistence layers:

1. **PostgreSQL (`asyncpg`):** The persistent, ACID-compliant source of truth for multi-tenant Job history, telemetry, and lineage metadata.
2. **Redis (`redis.asyncio`):** High-speed layer for Idempotency locks, Global Capacity Semaphores, Distributed Leader Election, and Lua-scripted Phase Barriers.
3. **RabbitMQ (`aio-pika`):** Dynamic, job-specific message queues for zero-trust task routing, Dead Letter Exchanges (DLX), and asynchronous worker status signaling.
4. **Kubernetes API:** Ephemeral infrastructure provisioning via dynamic Batch V1 Job manifests.

---

## ✨ Core Execution & Advanced Resilience

### 1. O(1) Memory Footprint Data Partitioning

Utilizes native Python Generators to calculate physical byte-range boundaries for massive datasets (e.g., multi-terabyte S3 objects). By yielding boundaries sequentially via the `Global Capacity Model`, the Orchestrator streams task assignments to RabbitMQ without ever allocating massive arrays in the Manager's local memory, completely preventing Out-Of-Memory (OOM) crashes during peak submission storms.

### 2. Multi-Tenant Sandboxing & Dynamic Binding

Data and computation are strictly isolated. The Manager dynamically derives Java execution class structures directly from the S3 user-code paths at runtime. It injects mathematically bounded, tenant-isolated S3 write scopes (`s3://results/<user_id>/<job_id>/`) directly into the worker payload envelopes to prevent cross-tenant data corruption.

### 3. Stateful Barriers & Data Locality

* **Lua-Backed Barriers:** Utilizes atomic Redis Lua scripts to safely track Map and Reduce completion signals across thousands of concurrent workers, eliminating distributed race conditions during phase transitions.
* **Locality Scheduling:** Dynamically extracts ESS worker bind addresses during the Map phase and utilizes Kubernetes **Node Affinity** to schedule Reducer pods on the exact physical hardware holding the heaviest shuffle partitions, drastically reducing cross-rack network traffic.

### 4. Hybrid Retry & Fail-Fast Protection

* **Transient Infrastructure Faults:** Applies a bounded retry envelope for transient hardware or network errors, automatically provisioning replacement Kubernetes compute transparently to the user.
* **Fail-Fast (Poison Pills):** Detects deterministic user-code exceptions (e.g., `NullPointerException`) and immediately triggers an asynchronous Saga teardown. This prevents infinite retry loops from hijacking cluster resources.

### 5. Reactive Lineage Recovery (Surgical Healing)

If a Reducer experiences a `SHUFFLE_FETCH_FAILED` error, the Manager intercepts the precise failure topology. Instead of crashing the parent Job, it places the Reducer in a Redis waitlist and **surgically re-provisions** only the specific missing Map tasks, minimizing compute waste.

---

## 🤖 Autonomous Background Services

To guarantee cluster stability in an Active-Active high-availability deployment, the Manager runs robust daemon services governed by **Redis-backed Distributed Leader Election** (preventing split-brain scenarios):

* **The Watchdog (Garbage Collection):** A concurrent background sweep that detects stalled or orphaned jobs, forcefully reclaims Kubernetes resources (Pods/Queues), and releases mathematical capacity semaphores back to the global pool.
* **The Reconciler (State Drift Correction):** Periodically audits Redis capacity locks against the live Kubernetes API. If it detects a "Phantom Reservation" (Redis claims capacity but 0 pods exist due to manual K8s intervention), it forcefully synchronizes the PostgreSQL state to prevent the cluster from deadlocking.

---

## 🔬 Architectural Trade-offs & Future Scalability

Operating a distributed control plane requires balancing state consistency with high-throughput API demands. The Orchestrator architecture acknowledges the following limits and trade-offs:

1. **Redis CP vs. AP Guarantees:** We utilize Redis for distributed locking, phase barriers, and semaphores. While incredibly fast (AP), Redis standard clustering lacks the strict Consistency/Partition-Tolerance (CP) guarantees of consensus engines like `etcd` or Zookeeper. In a severe network partition scenario, split-brain locking is theoretically possible. A hyperscale enterprise iteration would migrate critical state semaphores to `etcd`.
2. **Kubernetes API Polling vs. Informers:** The `Reconciler` service periodically queries the Kubernetes API server to detect state drift. At a massive scale (e.g., managing 10,000+ simultaneous worker pods), this polling architecture will trigger K8s control plane rate-limiting. Future iterations should replace polling with Kubernetes **SharedInformers** (Watch APIs) for zero-latency, event-driven state tracking.
3. **Task Batching vs. Maximum Parallelism:** To protect the K8s control plane from Pod-Creation latency spikes, the `get_optimal_worker_count` function implements an autonomous 5:1 batching ratio. This trades maximum theoretical parallelism (spawning 100,000 pods for 100,000 tasks) for infrastructure stability, ensuring hardware is utilized efficiently rather than suffocating the K8s scheduler.

---

## 🛠 Technology Stack

* **Framework:** FastAPI (Asynchronous ASGI)
* **Language:** Python 3.11+
* **Database & ORM:** PostgreSQL (`asyncpg` / SQLAlchemy)
* **Message Broker:** RabbitMQ (`aio-pika`)
* **In-Memory Cache:** Redis (`redis.asyncio`)
* **Infrastructure:** Kubernetes Client (`kubernetes_asyncio`)

---

## 🚀 Getting Started

### Prerequisites

* Python 3.11+
* A running Kubernetes cluster (Minikube, kind, or standard K8s)
* Docker Compose (for local backing infrastructure)

### Local Infrastructure Setup

Before launching the API, ensure your backing services are online:

```bash
docker-compose up -d postgres rabbitmq redis minio

```

### Installation

1. **Clone the repository and navigate to the manager service:**

```bash
git clone <repository-url>
cd mapreduce-orchestrator

```

2. **Create and activate a virtual environment:**

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\Activate.ps1

```

3. **Install dependencies:**

```bash
pip install -r requirements.txt

```

4. **Initialize the Database Schema:**

```bash
Get-Content init-db.sql | docker exec -i local-postgres psql -U postgres -d dds_db

```

*(Adjust the command above if using Linux/macOS or a different container name).*

### Running the Service

Start the Orchestrator API using Uvicorn:

```bash
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload

```

* **API Documentation (Swagger UI):** `http://127.0.0.1:8000/docs`
* **Health Check:** `http://127.0.0.1:8000/health`

---

## ⚙️ Configuration (Environment Variables)

Configured via environment variables (see `app/core/config.py`). Create a `.env` file or export these in your shell:

| Variable | Description | Default |
| --- | --- | --- |
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://postgres:password@localhost:5432/dds_db` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379/0` |
| `RABBITMQ_URL` | RabbitMQ connection string | `amqp://guest:guest@localhost:5672/` |
| `K8S_NAMESPACE` | Kubernetes namespace for Workers | `default` |
| `MAX_TOTAL_WORKERS` | Max concurrent pods allowed globally | `50` |
| `CHUNK_SIZE_BYTES` | Default byte split for Map tasks | `134217728` (128MB) |
| `MAP_TO_REDUCE_RATIO` | Configurable Map to Reduce partition ratio | `3` |
| `MAP_TASKS_PER_POD` | Task batching size to amortize boot latency | `5` |
| `GLOBAL_JOB_TIMEOUT` | Time before Watchdog kills a stalled job | `3600` (seconds) |
| `MOCK_K8S` | Run with Mock infrastructure provider | `False` |

---

## 🧪 Testing

The repository includes a comprehensive `pytest` suite covering unit logic, repository mocking, and integration with backend infrastructure.

Run the test suite via:

```bash
pytest -v

```
---

*Project for the 2026 Distributed Systems class, Technical University of Crete.*