# Distributed MapReduce Execution Fabric (Worker Node)

This repository contains the high-performance parallel execution engine and core API for a distributed MapReduce framework orchestrated on Kubernetes. Designed for cloud-native elasticity, this stateless compute worker dynamically processes Map and Reduce tasks using advanced memory-safe sorting algorithms, resilient AMQP messaging, and Zero-Trust peer-to-peer data transfers.

---

## 📂 Project Structure

* **`mapreduce-core`**: The public developer library containing the strictly typed `Mapper`, `Reducer`, and `Context` data transfer objects and interfaces.
* **`mapreduce-worker`**: The ephemeral compute daemon that manages AMQP consumption, JVM sandboxing, dynamic bytecode localization, and distributed shuffles.

---

## 🏗️ Core Architecture & Execution Engine

### 1. Dynamic Parallelism (Container-Aware)

The execution engine utilizes Java's `ForkJoinPool` with dynamic CPU discovery. It automatically detects Kubernetes vCPU quotas and applies a configurable over-provisioning factor (`PARALLELISM_FACTOR`, default `2.0x`) to mask I/O latency. Work-stealing thresholds are dynamically calculated per chunk to prevent thread starvation.

### 2. Ephemeral JVM Sandboxing

To protect the primary worker daemon from heap exhaustion (OOM), Metaspace leaks, or malicious user code, all task payloads are executed within an isolated Child JVM process (`SandboxRunner`). Bytecode is dynamically fetched from the multi-tenant S3 staging bucket and loaded via a custom `URLClassLoader` that is entirely destroyed upon task completion, guaranteeing zero memory leaks across thousands of tasks.

### 3. O(1) Memory Spill-to-Disk & External Sorting

The engine is built to process massive (11GB+) datasets with a flat memory footprint:

* **Map Phase:** Implements a direct-to-disk streaming `Context` that bypasses heap buffering. Records are grouped in-memory and flushed sequentially to concurrent, LZ4-compressed partition streams, minimizing lock contention and OS-level disk thrashing.
* **Reduce Phase:** Utilizes a **Lazy-Evaluating K-Way External Merge Sort**. Compressed gRPC streams are decoded via Apache Commons (with native concatenated frame support) and merged via a `PriorityQueue`, feeding the user's `Reducer` line-by-line to survive extreme data skew.

### 4. Zero-Trust P2P Data Plane

Shuffle fetches between Reducer pods and remote Map nodes occur over high-speed gRPC streams. Every RPC request is authenticated via an **HMAC-SHA256 Token Gateway**. Stream writes are handled atomically (`.tmp` to `.lz4`) to prevent binary corruption on the receiving end.

---

## 🛡️ Fault Tolerance & Reliability

The worker acts as a resilient participant in the distributed fabric, handling failure states gracefully:

* **Reactive Lineage Recovery:** If a gRPC shuffle fetch fails, the worker intercepts the `StatusRuntimeException` and transmits a targeted error signal (e.g., `SHUFFLE_FETCH_FAILED_FROM_NODE:10.0.x.x:7337`) back to the Orchestrator for surgical upstream re-computation, preventing infinite loop failures.
* **Poison Pill Protection:** The AMQP consumer monitors the `x-delivery-count` header. Tasks exceeding 3 retries are NACKed, routed to a Dead Letter Exchange (DLX), and flagged to the Manager to prevent cluster deadlocks.
* **Resilience4j Integration:** S3/MinIO network partitions are mitigated using exponential random backoff retry registries.
* **Execution-Aware Scale-to-Zero:** A background watchdog monitors queue idle time, pausing its countdown during active sandbox computations, and gracefully terminating the K8s Pod when idle thresholds are breached.

---

## 🔬 Architectural Trade-offs & Future Scalability

Building a distributed execution engine requires balancing isolation, performance, and complexity. The following design choices were made with specific trade-offs in mind:

1. **Polyglot Extensibility vs. JVM Boot Latency:** We chose a strict process-per-task sandbox model (`ProcessBuilder`) rather than thread-level isolation. While this introduces JVM startup latency (amortized via Orchestrator task-batching), it guarantees absolute fault isolation and paves the architecture for a fully polyglot engine (allowing future C++ or Python execution binaries) without risking the core daemon.
2. **The N x M Shuffle Network Bottleneck:** Currently, Reducers execute direct peer-to-peer fetches from External Shuffle Service (ESS) nodes. While highly efficient for medium-scale clusters, scaling beyond ~1,000 nodes could introduce TCP Incast storms. Future iterations for petabyte-scale workloads would necessitate Map-side combiners or an intermediate distributed Key-Value store (e.g., Redis Cluster/Cassandra) for shuffle data.
3. **Merge-Sort CPU Saturation:** The K-Way external merge sort guarantees an O(1) memory footprint during the Reduce phase, but relies on a single-threaded Priority Queue pull. For extreme, Zipfian-skewed datasets where a single partition exceeds 100GB, this single-threaded aggregation would eventually bottleneck NVMe drive read speeds. A future enhancement would implement a multithreaded parallel merge-tree.

---

## ⚙️ Configuration (Environment Variables)

The worker behaves as a sidecar to the External Shuffle Service (ESS) and relies on environment variable injection:

| Variable | Description | Default |
| --- | --- | --- |
| `JOB_ID` | UUID of the job (Used for AMQP queue isolation). | *(Required)* |
| `PHASE` | Current workload phase (`map` or `reduce`). | `map` |
| `RABBITMQ_HOST` | Hostname for the AMQP control plane. | `localhost` |
| `MINIO_ENDPOINT` | HTTP endpoint for the S3-compatible cluster. | `http://localhost:9000` |
| `NODE_IP` | Physical Host IP for P2P data locality routing (via Downward API). | `127.0.0.1` |
| `SHUFFLE_DIR` | Shared HostPath volume mount for ESS handoff. | `/mnt/mapreduce-shuffle` |
| `PARALLELISM_FACTOR` | Multiplier for ForkJoin thread provisioning. | `2.0` |
| `IDLE_TIMEOUT_MILLIS` | Inactivity window before graceful pod termination. | `5000` |

---

## 🚀 Building & Deployment

### Build the Uber-JAR

The project is built as a multi-module Maven repository. Building from the root installs the core library and packages the worker.

```bash
mvn clean package

```

### Containerization

The Dockerfile utilizes a multi-stage build, resulting in a lightweight `eclipse-temurin:17-jre` production image.

```bash
cd mapreduce-worker
docker build -t mapreduce-worker:v3 .

```

---

## 📜 Developer API: Writing Jobs

Developers can integrate the `mapreduce-core` library via **JitPack** to implement custom logic.

### Dependency (`pom.xml`)

```xml
<repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
</repository>

<dependency>
    <groupId>com.github.georgekarabaggelisjr.distributedsystemsproject</groupId>
    <artifactId>mapreduce-core</artifactId>
    <version>v2.0</version>
</dependency>

```

### Implementing a Mapper & Reducer

```java
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.core.Context;
import com.iliasbolan.core.KeyValuePair;
import java.util.Iterator;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String chunk, Context context) {
        String[] words = chunk.toLowerCase().replaceAll("[^a-z0-9]+", " ").split("\\s+");
        for (String word : words) {
            if (word.length() > 1) { 
                context.write(word, "1"); // Direct-to-disk streaming Context
            }
        }
    }
}

public class WordCountReducer implements Reducer {
    @Override
    public KeyValuePair reduce(String key, Iterator<String> values) {
        long total = 0;
        // OOM-Safe Iterator processing for massive key-skew
        while (values.hasNext()) {
            total += Long.parseLong(values.next());
        }
        return new KeyValuePair(key, String.valueOf(total));
    }
}

```

---

*Project for the 2026 Distributed Systems class, Technical University of Crete.*