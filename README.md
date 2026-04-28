# External Shuffle Service (ESS)

[![Java Support](https://img.shields.io/badge/Java-17%2B-blue.svg)](https://www.oracle.com/java/)
[![gRPC](https://img.shields.io/badge/gRPC-Stream-4285F4.svg)](https://grpc.io/)
[![Kubernetes](https://img.shields.io/badge/K8s-DaemonSet-326CE5.svg)](https://kubernetes.io/)
[![Security](https://img.shields.io/badge/Security-HMAC--SHA256-red.svg)]()

The **External Shuffle Service (ESS)** is a critical infrastructure component of the MapReduce framework. It serves as a dedicated, high-performance data plane that decouples shuffle data storage from the ephemeral lifecycle of worker pods.

By running as a permanent **DaemonSet** on every Kubernetes node, the ESS ensures that intermediate Map output remains available for Reducers even after the original Mapper pods have terminated.

---

## 🏗️ Core Architecture & Features

* **High-Performance Streaming:** Implements a gRPC server-streaming RPC (`GetPartition`) to transmit intermediate data chunks using **Java NIO Direct Buffers (64KB)**, minimizing context switching and heap pressure.
* **Zero-Copy Strategy:** Utilizes `FileChannel` to stream partition segments directly from the host filesystem to the network buffer.
* **Zero-Trust Security:** Every request is validated via an **HMAC-SHA256 gateway**. Reducers must provide a Manager-issued token in the gRPC metadata, which the ESS verifies against a shared secret to prevent unauthorized data access.
* **Storage Janitor (GC):** A background garbage collection service that periodically scans the shuffle directory and purges data from completed or abandoned jobs to prevent disk exhaustion.
* **Flow Control Awareness:** The streaming implementation respects gRPC backpressure signals, utilizing a spin-lock wait mechanism to ensure stable transmission across varying network conditions.

---

## 🔐 Security Protocol

The ESS implements a cryptographic barrier to protect multi-tenant data:
1.  **Token Generation:** The Orchestrator generates an HMAC-SHA256 signature using the `JOB_ID` and a `SECRET_KEY`.
2.  **Transmission:** The Reducer injects this token into the `authorization` header.
3.  **Verification:** The ESS re-computes the HMAC and performs a **constant-time comparison** (`MessageDigest.isEqual`) to authorize the fetch.

---

## ⚙️ Configuration (Environment Variables)

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ESS_PORT` | The gRPC listening port. | `7337` |
| `SHUFFLE_DIR` | Local path where Map outputs are stored. | `/mnt/mapreduce-shuffle` |
| `ESS_SECRET_KEY` | Secret key used for HMAC token verification. | `dev-insecure-shared-secret` |
| `GC_INTERVAL_MIN` | Frequency of the Storage Janitor cleanup cycle. | `60` (minutes) |
| `RETENTION_HOURS` | How long to keep shuffle data before deletion. | `24` (hours) |

---

## 📦 Deployment

The ESS is designed to run as a **Kubernetes DaemonSet** to provide data locality for every node in the cluster.

### Kubernetes Highlight:
* **Host Networking:** Binds directly to the node's IP via `hostPort: 7337`.
* **Priority:** Uses `system-cluster-critical` priority to prevent eviction during resource pressure.
* **Storage:** Mounts a physical `hostPath` volume at `/var/lib/mapreduce/shuffle-data`.

```bash
# Apply the infrastructure template
kubectl apply -f app/k8s/templates/ess-daemonset.yaml