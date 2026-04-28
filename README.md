# Distributed MapReduce Execution Fabric

[![Java Support](https://img.shields.io/badge/Java-17%2B-blue.svg)](https://www.oracle.com/java/)
[![Maven Central](https://img.shields.io/badge/Maven-3.8%2B-C71A36.svg)](https://maven.apache.org/)
[![JitPack](https://jitpack.io/v/georgekarabaggelisjr/distributedsystemsproject.svg)](https://jitpack.io/#georgekarabaggelisjr/distributedsystemsproject)
[![Architecture](https://img.shields.io/badge/Architecture-Stateless_Compute-brightgreen.svg)]()

This repository contains the high-performance parallel execution engine and core API for a distributed MapReduce framework orchestrated on Kubernetes. The system is split into a multi-module Maven project to separate the developer-facing API from the internal worker logic.

## 📂 Project Structure

* **`mapreduce-core`**: The public library containing the `Mapper`, `Reducer`, and `Context` interfaces.
* **`mapreduce-worker`**: The ephemeral compute node that localizes bytecode, manages JVM sandboxing, and performs P2P shuffles.
---

## 📦 Developer Integration (JitPack)

To write custom MapReduce jobs, developers should include the `mapreduce-core` library as a dependency.

### 1. Add the Repository
Add the JitPack repository to your `pom.xml`:
```xml
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>[https://jitpack.io](https://jitpack.io)</url>
    </repository>
</repositories>
```
### 2. Add the Dependency
Target the `mapreduce-core` submodule specifically. Replace `v2.0.0` with your latest GitHub Release Tag or feature-java-worker-SNAPSHOT` for the absolute latest commit:
```xml
<dependency>
    <groupId>com.github.georgekarabaggelisjr.distributedsystemsproject</groupId>
    <artifactId>mapreduce-core</artifactId>
    <version>v2.0</version>
</dependency>
```

## 🏗️ Core Architecture

* **`Dynamic Isolation`**: Workers utilize job-specific AMQP queues (`map_tasks_{job_id}`) to ensure zero cross-talk in multi-tenant environments.
* **`JVM Sandboxing`**: Untrusted user code is executed in a forked child JVM process to protect the main worker from heap exhaustion or system-level crashes.
* **`Zero-Trust Shuffle`**: High-performance gRPC streams between Reducers and the ESS are protected by HMAC-SHA256 tokens issued by the global Manager.
* **`Fault Tolerance`**: Includes a Lineage Recovery mechanism where failed shuffle fetches trigger a targeted re-publication of the missing Map chunk.

## ⚙️ Deployment Configuration

The worker is configured via environment variables, typically injected through Kubernetes manifests.

| Variable         | Function                                                                 | Default                      |
|-----------------|-------------------------------------------------------------------------|------------------------------|
| JOB_ID          | UUID of the job (Used for queue and storage isolation).                 | null                         |
| PHASE           | Current workload phase (map or reduce).                                 | map                          |
| RABBITMQ_HOST   | Connectivity for the AMQP control plane.                                | localhost                    |
| NODE_IP         | Physical Host IP for ESS routing (via Downward API).                    | 127.0.0.1                    |
| SHUFFLE_DIR     | Shared volume mount for intermediate data spills.                       | /mnt/mapreduce-shuffle       |
| ESS_SECRET_KEY  | Shared secret for gRPC HMAC token verification.                         | dev-insecure-shared-secret   |

## 🚀 Getting Started

### Build the Project

Building from the root installs all modules, including the core library needed by the worker.

```bash
mvn clean install
```
### Build the Docker Image

```bash
# Build the compute worker
cd mapreduce-worker
docker build -t mapreduce-worker:v10 .
```

### Local Testing

The suite utilizes JUnit 5 and Mockito to validate everything from AMQP Poison Pill protections to the `ExternalMergeSorter` logic.
```bash 
mvn test
```

## 📜 Interface Example

Implement your logic by extending the core interfaces:

```java 
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Context;
import java.util.HashMap;
import java.util.Map;

public class WordCountMapper implements Mapper {

    @Override
    public void map(String chunkOfText, Context context) {
        Map<String, Long> localCounts = new HashMap<>();

        String[] words = chunkOfText.toLowerCase().replaceAll("[^a-z0-9]+", " ").split("\\s+");
        
        for (String word : words) {
            if (word.length() > 1) { 
                localCounts.put(word, localCounts.getOrDefault(word, 0L) + 1L);
            }
        }

        for (Map.Entry<String, Long> entry : localCounts.entrySet()) {
            context.write(entry.getKey(), String.valueOf(entry.getValue()));
        }
    }
}
```
```java 
import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import java.util.Iterator;

public class WordCountReducer implements Reducer {

    @Override
    public KeyValuePair reduce(String key, Iterator<String> values) {
        long totalCount = 0;

        while (values.hasNext()) {
            try {
                totalCount += Long.parseLong(values.next());
            } catch (NumberFormatException e) {}
        }

        return new KeyValuePair(key, String.valueOf(totalCount));
    }
}
```

Project for the 2026 Distributed Systems class for the Technical Univeristy of Crete.
