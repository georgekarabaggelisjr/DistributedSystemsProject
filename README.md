# External Shuffle Service (ESS)

[![Java Support](https://img.shields.io/badge/Java-17%2B-blue.svg)](https://www.oracle.com/java/)
[![gRPC](https://img.shields.io/badge/gRPC-Stream-4285F4.svg)](https://grpc.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49.svg)](https://min.io/)
[![Kubernetes](https://img.shields.io/badge/K8s-DaemonSet-326CE5.svg)](https://kubernetes.io/)

The **External Shuffle Service (ESS)** is a resilient, high-performance data plane designed for the MapReduce framework. It runs as a permanent **DaemonSet** on every Kubernetes node, decoupling intermediate shuffle data storage from the ephemeral lifecycle of worker pods. This ensures that intermediate Map output remains available for Reducers even after the original Mapper pods have terminated or scaled down.

---

## 🏗️ Core Architecture & Features

* **High-Performance Streaming:** Implements a gRPC server-streaming RPC (`GetPartition`) to transmit intermediate data chunks using **Java NIO Direct Buffers (65KB)**, guaranteeing Protobuf envelopes fit inside strict HTTP/2 flow-control windows while minimizing heap pressure.
* **LZ4 Pass-Through Optimization:** Acts as a transparent binary pipeline, streaming raw `.lz4` partition segments directly from the host filesystem to the network wire. Bypassing decompression preserves host CPU resources.
* **Zero-Copy Strategy:** Utilizes `FileChannel` to stream partition blocks directly into direct memory buffers, respecting gRPC backpressure signals via spin-lock wait mechanisms for stable transmission.
* **Zero-Trust Security:** Every request is strictly validated via an **HMAC-SHA256 gateway**. Reducers must provide a Manager-issued authorization token which the ESS verifies via constant-time comparison against a shared secret.

---

## 💾 Multi-Tier Storage & Intelligent Janitor

To prevent node-level disk exhaustion in multitenant or high-throughput environments, the ESS integrates a **Space-Aware Garbage Collector (Janitor)** alongside a **MinIO (S3-compatible) Overflow Tier**.

1. **High-Resolution Monitoring:** The Janitor continuously polls the underlying NIO `FileStore` every 30 seconds.
2. **Dual-Watermark Reclamation:**
    * **High-Watermark (85%):** When disk utilization breaches 85%, aggressive reclamation triggers. The system identifies oldest job directories and **spills** their `.lz4` payloads to the MinIO object store before physically deleting them from the local disk.
    * **Low-Watermark (70%):** Deletions halt once local storage returns to this safe buffer.
3. **Lazy-Restoration:** If a straggler Reducer requests a partition that was evicted locally, the gRPC data plane performs a real-time, multi-segment restoration from the MinIO bucket, ensuring zero data loss.
4. **Hard TTL Sweep:** A fallback mechanism aggressively purges any job data older than 24 hours without spilling to S3.

---

## 🔐 Security Protocol

The ESS implements a cryptographic barrier to protect multi-tenant data:
1. **Token Generation:** The Orchestrator generates an HMAC-SHA256 signature using the `JOB_ID` and a `SECRET_KEY`.
2. **Transmission:** The Reducer injects this token into the `authorization` header.
3. **Verification:** The ESS global gRPC interceptor re-computes the HMAC, validating the identity and binding it to a secure Thread-Local context. It also sanitizes the `JOB_ID` against arbitrary path traversal attacks.

---

## ⚙️ Configuration (Environment Variables)

The ESS daemon degrades gracefully. If MinIO variables are misconfigured, it will operate in a local-only mode (risking data loss upon physical disk exhaustion).

| Variable | Description | Default |
| :--- | :--- | :--- |
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

The Dockerfile utilizes `eclipse-temurin:17-jre-alpine` for a lightweight production footprint:

```bash
docker build -t mapreduce-ess:v2 .

```

---

## 📦 Kubernetes Deployment

The ESS is designed to run as a **DaemonSet** to provide data locality.

* **Host Networking:** Binds directly to the node's IP via `hostPort: 7337`.
* **Storage Mount:** Uses a `hostPath` volume mapped to `/var/lib/mapreduce/shuffle-data`.
* **Priority Class:** Deployed with `system-cluster-critical` to prevent eviction during resource starvation.

```bash
kubectl apply -f app/k8s/templates/ess-daemonset.yaml

```

---

## 📜 API Definition (gRPC)

The peer-to-peer data transfer service hosted by Map workers:

```proto
syntax = "proto3";
package shuffle;
option java_package = "com.iliasbolan.grpc.shuffle";

service ShuffleService {
  // The Reducer requests a specific partition, and the Map worker
  // streams the intermediate data back in chunks.
  rpc GetPartition (PartitionRequest) returns (stream PartitionChunk);
}

// The request sent by the Reduce worker.
message PartitionRequest {
  string job_id = 1;
  
  // The ID of the partition the Reducer is assigned to (e.g., 0, 1, 2).
  // This typically corresponds to the Reducer's Task ID.
  int32 partition_id = 2;
}

// A binary chunk of the intermediate data payload.
message PartitionChunk {
  bytes content = 1;
}

```

---

*Developed by Ilias Bolanakis for the 2026 Distributed Systems class, Technical University of Crete.*
