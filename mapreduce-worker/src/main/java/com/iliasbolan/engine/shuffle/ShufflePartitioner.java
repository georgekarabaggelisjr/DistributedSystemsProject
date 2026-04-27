package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.engine.execution.MapTaskProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles the Shuffle and Partitioning phase of a Map-Reduce job.
 * * <p><b>Architectural Update: Spill-to-Disk Architecture</b><br>
 * To facilitate memory-safe Map phases and prevent {@link OutOfMemoryError} conditions,
 * this class supports highly concurrent, thread-safe appends. It implements
 * <i>Lock Stripping</i>—utilizing an array of dedicated locks per partition—to
 * allow multiple {@link java.util.concurrent.ForkJoinPool} threads to flush their
 * localized buffers to the container's disk in parallel without risk of data corruption.</p>
 * * <p><b>Architectural Update: Peer-to-Peer (P2P) gRPC</b><br>
 * This component writes partitioned output directly to the local container file system
 * using Java NIO. These deterministic file paths are subsequently exposed via an
 * embedded gRPC server, enabling remote Reduce nodes to perform direct data
 * transfers from sibling Map nodes.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 * @see MapTaskProcessor
 */
public class ShufflePartitioner {

    private static final Logger logger = LoggerFactory.getLogger(ShufflePartitioner.class);

    private final String baseShuffleDir;
    private final String jobId;
    private final String mapTaskId;
    private final int numReducers;

    /** * Internal array of granular locks used for Lock Stripping.
     * This mechanism ensures that threads writing to distinct partitions (e.g., Partition 0
     * vs. Partition 1) do not contend for the same monitor lock.
     */
    private final Object[] partitionLocks;

    /**
     * Initializes a new {@code ShufflePartitioner} instance for a specific Map task
     * and configures concurrent local storage mechanisms.
     *
     * @param baseShuffleDir The root directory on the local disk designated for shuffle data storage.
     * @param jobId          The unique identifier for the current Map-Reduce job execution.
     * @param mapTaskId      The unique identifier for the specific Map chunk being processed.
     * @param numReducers    The total number of Reducer partitions (R) configured for this job.
     */
    public ShufflePartitioner(String baseShuffleDir, String jobId, String mapTaskId, int numReducers) {
        this.baseShuffleDir = baseShuffleDir;
        this.jobId = jobId;
        this.mapTaskId = mapTaskId;
        this.numReducers = numReducers;

        // Initialize granular locks for thread-safe file appends
        this.partitionLocks = new Object[numReducers];
        for (int i = 0; i < numReducers; i++) {
            this.partitionLocks[i] = new Object();
        }

        logger.info("Initialized ShufflePartitioner (P2P Local Storage). JobId: {}, MapTaskId: {}, Target Reducers: {}",
                jobId, mapTaskId, numReducers);
    }

    /**
     * Persists a batch of mapped records directly to the local file system in a
     * thread-safe manner.
     * * <p>This method performs a thread-local grouping of the incoming buffer by
     * target partition. It then acquires a granular lock specific to each
     * partition file before performing an atomic append operation. This design
     * minimizes lock contention while guaranteeing file integrity.</p>
     *
     * @param buffer A memory-bounded collection of {@link KeyValuePair} objects
     * emitted by a single computation thread.
     * @throws IOException If a file system error occurs during directory creation
     * or the data append process.
     */
    public void appendThreadSafe(List<KeyValuePair> buffer) throws IOException {
        // Step 1: Group the incoming buffer by target partition (Thread-Local operation)
        Map<Integer, StringBuilder> localPartitions = new HashMap<>();

        for (KeyValuePair pair : buffer) {
            // Correctly ensures a positive integer by stripping the sign bit
            int partitionIndex = (pair.key().hashCode() & Integer.MAX_VALUE) % numReducers;

            localPartitions.computeIfAbsent(partitionIndex, k -> new StringBuilder())
                    .append(pair.key()).append("\t").append(pair.value()).append("\n");
        }

        // Step 2: Flush to disk using granular locks
        for (Map.Entry<Integer, StringBuilder> entry : localPartitions.entrySet()) {
            int partitionIndex = entry.getKey();
            String payload = entry.getValue().toString();

            Path partitionDir = Paths.get(baseShuffleDir, jobId, String.valueOf(partitionIndex));
            Path filePath = partitionDir.resolve(mapTaskId + ".txt");

            // Acquire the dedicated lock for this specific partition
            synchronized (partitionLocks[partitionIndex]) {
                if (!Files.exists(partitionDir)) {
                    Files.createDirectories(partitionDir);
                }

                // Explicitly use UTF-8 to prevent multibyte boundary errors
                Files.writeString(
                        filePath,
                        payload,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
            }
        }
    }

    /**
     * Legacy method for partitioning a bulk list of intermediate key-value pairs.
     * * @param intermediateData The complete collection of intermediate data.
     * @throws IOException If a file system error occurs.
     * @deprecated As of version 2.0, replaced by {@link #appendThreadSafe(List)}
     * to support memory-safe streaming and prevent OOM scenarios.
     */
    @Deprecated
    public void partitionAndWriteLocal(List<KeyValuePair> intermediateData) throws IOException {
        logger.warn("Invoked deprecated partitionAndWriteLocal. Use appendThreadSafe for OOM protection.");
        appendThreadSafe(intermediateData);
    }
}