# Distributed Map-Reduce Worker Node

A highly concurrent, cloud-native worker node designed to execute dynamic Map-Reduce workloads in a distributed containerized environment. Built strictly with **Java 17**, this engine leverages native Fork/Join parallelism to maximize multi-core CPU utilization while communicating seamlessly with RabbitMQ and MinIO (S3).

## Architectural Overview

Unlike monolithic processing scripts, this worker operates as a stateless compute engine. It dynamically downloads user-defined logic and data chunks at runtime, executes the computation, and gracefully shuts down.

### The Execution Pipeline
1. **Task Consumption (2-Queue Architecture):** Listens to a **RabbitMQ** Work Queue for structured JSON task assignments, while concurrently publishing real-time telemetry (success/fail states) to an **Event/Audit Queue** for downstream orchestration.
2. **Resource Acquisition:** Uses the **MinIO S3 Client** to download the designated 64MB data chunk and the user's compiled `.class` code.
3. **Dynamic Reflection:** Instantiates the user's custom `Mapper` or `Reducer` securely at runtime using a custom `URLClassLoader`.
4. **Parallel Computation:** Bypasses legacy thread pools in favor of Java's **Fork/Join Framework**, dynamically calculating optimal split thresholds to utilize work-stealing across all available CPU cores.
5. **Shuffle & Sort (Reduce Phase):** Enforces strict deterministic routing via Hash-Modulo math and guarantees "Total Order" semantics by sorting keys alphabetically before final reduction.
6. **Persistence:** Serializes and streams intermediate fragments or final output back to the MinIO shared file system.

## Key Engineering Features

* **Work-Stealing Parallelism:** Both Map and Reduce phases implement `RecursiveTask`. The engine automatically detects Kubernetes/Docker CPU limits via `Runtime.getRuntime().availableProcessors()` and splits large datasets into perfectly sized sub-tasks to prevent thread starvation.
* **Asymmetric Dynamic Loading:** Users do not need to recompile the Worker to run new jobs. The `DynamicClassLoader` fetches compiled byte-code from S3 and casts it to strict `Mapper` or `Reducer` interfaces at runtime.
* **Cloud-Native Graceful Termination:** Implements a background Daemon thread that monitors RabbitMQ queue activity. If idle for a configurable timeout, it gracefully interrupts the main thread, closes TCP sockets, and exits with status 0, saving cloud compute costs.
* **Idempotent Data Routing:** The `ShufflePartitioner` uses strict `Math.abs(hash) % R` logic to guarantee that identical keys consistently route to identical intermediate S3 objects across thousands of distributed nodes.
* **Immutable Data Transfer:** Utilizes **Java 17 Records** (`TaskPayload`, `KeyValuePair`) to ensure absolute thread-safety and eliminate race conditions during high-speed parallel grouping.
* **Worker Termination:** If the worker pod stays inactive for a few seconds it auto-terminates.

## Tech Stack

* **Core Language:** Java 17 (Records, Pattern Matching)
* **Messaging:** RabbitMQ (AMQP Client)
* **Shared File System:** MinIO (S3 SDK)
* **Serialization:** Jackson (JSON)
* **Logging:** SLF4J + Logback
* **Testing:** JUnit 5 + Mockito 5.x + Byte Buddy


## Important Constraints
### Encoding Requirements
This engine is optimized for high-performance distributed processing and strictly supports **UTF-16** encoded input files.
* Ensure all data uploaded to MinIO is saved as UTF-16.
* Data encoded in other formats (e.g., UTF-8, ASCII) may result in character corruption or word fragmentation errors during the Map phase.


## Testing Pyramid & Quality Assurance

This project maintains a rigorous, Enterprise-grade testing suite designed to prove mathematical correctness and network resilience:

* **Unit Tests (Core & Math):** Validates Fork/Join aggregation, JSON deserialization, and deterministic hashing using `ArgumentCaptor` to inspect internal state.
* **Orchestration Tests (TaskExecutor):** Utilizes `MockedStatic` to intercept the ClassLoader and test the entire Map and Reduce pipeline completely isolated from the file system.
* **Integration Tests (Network):** Connects to live Docker containers to verify RabbitMQ timeouts, exact byte-range S3 chunking, and ACK/NACK message broker logic.

## Getting Started (Local Development)

### 1. Boot up the Infrastructure
Ensure Docker Desktop is running, then spin up the local RabbitMQ and MinIO containers:
```bash
docker-compose up -d
```

* **MinIO Console:** `http://localhost:9001` (admin/admin)
* **RabbitMQ Management:** `http://localhost:15672` (guest/guest)

### 2. Run the Test Suite
Prove the architecture is sound by running the complete suite:
```bash
mvn clean test
```

### 3. Build the Artifact
To package the worker node into a runnable JAR file for deployment:
```bash
mvn clean package
```

Architected and developed by Ilias Bolanakis.