# External Shuffle Service (ESS)

The **External Shuffle Service (ESS)** is a resilient, high-performance data plane designed for the distributed MapReduce framework. Operating as a permanent **DaemonSet** across all Kubernetes nodes, it structurally decouples intermediate shuffle data storage from the ephemeral compute lifecycle of worker pods. This architectural pattern guarantees that intermediate Map outputs remain highly available for Reducers, enabling aggressive Map-side scale-to-zero capabilities without data loss.

---

## 🏗️ Core Architecture & Performance Tuning

* **High-Performance Streaming:** Implements a gRPC server-streaming RPC (`GetPartition`) to transmit intermediate data chunks using **Java NIO Direct Buffers (65KB)**. This strictly aligns Protobuf payload envelopes with HTTP/2 flow-control window constraints, preventing network-layer fragmentation.
* **LZ4 Pass-Through Optimization:** Acts as a transparent binary pipeline. By streaming raw, pre-compressed `.lz4` partition segments directly from the host filesystem to the network wire, the ESS completely bypasses decompression, shielding the host node's CPU from computationally expensive inflation tasks.
* **Zero-Copy & Backpressure Propagation:** Utilizes `FileChannel` to map partition blocks directly into off-heap direct memory. To prevent Out-Of-Memory (OOM) crashes during massive P2P network congestion, the ESS respects gRPC client backpressure signals via a tuned spin-lock wait mechanism, yielding execution only when the HTTP/2 transport buffer is saturated.
* **Zero-Trust Security Plane:** Reducers execute cross-node fetches over an untrusted network. Every RPC is validated via a strict **HMAC-SHA256 Token Gateway**. The ESS global gRPC interceptor re-computes and verifies the Orchestrator-issued token against a symmetric shared secret in constant time, preventing replay and path-traversal attacks.

---

## 💾 Multi-Tier Storage & Intelligent Janitor

In multitenant Kubernetes clusters, utilizing raw `hostPath` volumes risks catastrophic node-level disk exhaustion. To mitigate this without relying on expensive SAN/NAS persistent volumes, the ESS integrates a **Space-Aware Garbage Collector (StorageJanitor)** paired with a **MinIO (S3-Compatible) Overflow Tier**.

1. **High-Resolution Telemetry:** The StorageJanitor continuously polls the underlying physical NIO `FileStore` at 30-second intervals.
2. **Dual-Watermark Reclamation:**
* **High-Watermark (85%):** Upon breaching safe disk capacity, aggressive reclamation triggers. The Janitor identifies the oldest idle job directories and **spills** their `.lz4` payloads to the MinIO object store before physically purging them from local NVMe/SSD storage.
* **Low-Watermark (70%):** Deletions gracefully halt once the local storage returns to this operational buffer.


3. **Lazy-Restoration (Cache Miss Handling):** If a straggler Reducer requests a partition that was previously evicted to the Overflow Tier, the ESS intercepts the cache miss and performs a real-time, multi-segment restoration from the MinIO bucket back to the local disk, guaranteeing zero data loss.
4. **Hard TTL Sweep:** A fallback safety mechanism unconditionally purges any job artifacts older than 24 hours to prevent orphan data leaks from aborted orchestrator jobs.

---

## 🔬 Architectural Trade-offs & Future Scalability

Designing an External Shuffle Service requires balancing node-level resource contention against distributed reliability. The current architecture acknowledges the following trade-offs:

1. **Custom Janitor GC vs. K8s Local Persistent Volumes (LPV):** We utilized a standard `hostPath` mount coupled with our custom `StorageJanitor` daemon to manage disk capacity. While Kubernetes LPVs provide native capacity enforcement, they introduce severe scheduling rigidity. Our custom High/Low watermark approach trades software complexity for scheduling elasticity, allowing standard K8s clusters to run massive jobs without complex storage provisioning.
2. **Spin-Lock Backpressure vs. Asynchronous Reactive Streams:** To handle gRPC network backpressure (`isReady()`), the ESS utilizes a spin-lock wait loop. This heavily favors low-latency environments by keeping the thread hot and resuming I/O instantly when the network buffer clears. However, under extreme, prolonged network congestion, this burns CPU cycles. A future enterprise iteration would migrate this to a fully reactive, asynchronous event loop (e.g., using Netty's reactive bindings) to improve CPU efficiency at the cost of slight context-switching overhead.
3. **Node-Level Ephemerality:** The ESS provides durability against *Pod* failures, but not total physical *Node* failures. If an entire EC2 instance or physical blade dies, the local ESS data is lost unless the Janitor had already spilled it to S3. The framework accepts this trade-off, relying on the Orchestrator's **Reactive Lineage Recovery** (re-running the specific lost Map tasks) rather than paying the massive network cost of 3x replicating all intermediate shuffle data via Raft/Paxos.

---

## ⚙️ Configuration (Environment Variables)

The ESS daemon degrades gracefully. If MinIO variables are misconfigured or unreachable, it will operate in a local-only mode (relying solely on 24-hour TTLs rather than S3 spilling).

| Variable | Description | Default |
| --- | --- | --- |
| `ESS_PORT` | The gRPC listening port. | `7337` |
| `SHUFFLE_DIR` | Local host path where Map outputs are stored. | `/mnt/mapreduce-shuffle` |
| `ESS_SECRET_KEY` | Secret key used for HMAC token verification. | `dev-insecure-shared-secret` |
| `MINIO_ENDPOINT` | The HTTP endpoint for the MinIO/S3 cluster. | `http://minio:9000` |
| `MINIO_ACCESS_KEY` | Admin access key for the S3 bucket. | `minioadmin` |
| `MINIO_SECRET_KEY` | Admin secret key for the S3 bucket. | `minioadmin` |
| `MINIO_BUCKET` | The bucket name used for the overflow tier. | `ess-overflow` |

---

## 🚀 Building & Containerization

### 1. Build the Fat JAR

Compile the project and generate the Protobuf/gRPC stubs using Maven:

```bash
mvn clean package

```

### 2. Build the Docker Image

The Dockerfile utilizes `eclipse-temurin:21-jre-alpine` for a lightweight, production-ready footprint:

```bash
docker build -t mapreduce-ess:v2 .

```

---

## 📦 Kubernetes Deployment

The ESS is designed to run as a **DaemonSet** to provide guaranteed data locality on every compute node.

* **Host Networking:** Binds directly to the node's physical IP via `hostPort: 7337` to bypass `kube-proxy` overhead.
* **Storage Mount:** Uses a `hostPath` volume mapped to `/var/lib/mapreduce/shuffle-data`.
* **Priority Class:** Deployed with `system-cluster-critical` to prevent Kubernetes from evicting the ESS during extreme memory pressure.

```bash
kubectl apply -f app/k8s/templates/ess-daemonset.yaml

```

---

## 📜 API Definition (gRPC)

The core contract for peer-to-peer intermediate data transfer:

```proto
syntax = "proto3";
package shuffle;
option java_package = "com.iliasbolan.grpc.shuffle";

service ShuffleService {
  // Reducer requests a specific partition. The ESS streams the pre-compressed 
  // intermediate data back in 65KB Direct Memory chunks.
  rpc GetPartition (PartitionRequest) returns (stream PartitionChunk);
}

// The request dispatched by the remote Reduce worker.
message PartitionRequest {
  string job_id = 1;
  
  // The numeric ID of the partition the Reducer is assigned to process.
  int32 partition_id = 2;
}

// A binary payload chunk. Encapsulates raw LZ4 frames.
message PartitionChunk {
  bytes content = 1;
}

```

---

*Project for the 2026 Distributed Systems class, Technical University of Crete.*