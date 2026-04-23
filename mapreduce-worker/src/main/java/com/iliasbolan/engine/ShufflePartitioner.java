package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
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
 * <p>
 * <b>Spill-to-Disk Architecture Update:</b><br>
 * To support the OOM-safe Map phase, this class has been upgraded to support highly
 * concurrent, thread-safe appends. It uses Lock Stripping (an array of dedicated locks
 * per partition) to allow multiple Fork/Join threads to flush their localized buffers
 * to the container's disk in parallel without corrupting the target text files.
 * </p>
 * <p>
 * <b>Peer-to-Peer (P2P) gRPC Architecture Update:</b><br>
 * This class writes the partitioned output directly to the local container disk using
 * standard Java NIO. These deterministic file paths are subsequently streamed by the
 * embedded gRPC server, allowing sibling Reduce nodes to fetch the data directly.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @see com.iliasbolan.engine.MapTaskProcessor
 * @since 2026-04-24
 */
public class ShufflePartitioner {

    private static final Logger logger = LoggerFactory.getLogger(ShufflePartitioner.class);

    private final String baseShuffleDir;
    private final String jobId;
    private final String mapTaskId;
    private final int numReducers;

    /** * Array of granular locks. Lock Stripping ensures that threads writing to
     * partition 0 do not block threads writing to partition 1.
     */
    private final Object[] partitionLocks;

    /**
     * Constructs a new {@code ShufflePartitioner} for a specific Map task.
     *
     * """
     * Initializes the P2P Local Storage partitioner and its concurrent locking mechanisms.
     * * Args:
     * baseShuffleDir (str): The root directory on the local disk for shuffle data.
     * jobId (str): The unique identifier for the current Map-Reduce job.
     * mapTaskId (str): The unique identifier for the specific Map chunk being processed.
     * numReducers (int): The total number of Reducer partitions configured for this job (R).
     * """
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
     * Thread-safe method to append a batch of mapped records directly to local disk.
     *
     * """
     * Flushes a memory-bounded buffer of KeyValuePairs to their respective partition files.
     * * This method groups the incoming buffer locally, then acquires the specific lock
     * for each target partition file before appending data. This prevents file corruption
     * while maintaining high concurrency.
     * * Args:
     * buffer (List[KeyValuePair]): A small batch of processed records from a single Fork/Join thread.
     * * Raises:
     * IOException: If a file system error occurs during the directory creation or append process.
     * """
     */
    public void appendThreadSafe(List<KeyValuePair> buffer) throws IOException {
        // Step 1: Group the incoming buffer by target partition (Thread-Local operation)
        Map<Integer, StringBuilder> localPartitions = new HashMap<>();

        for (KeyValuePair pair : buffer) {
            // Use Math.abs to ensure the hash is positive before the modulo operation
            int partitionIndex = Math.abs(pair.key().hashCode()) % numReducers;

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

                // Explicitly use UTF-8 to prevent multi-byte boundary errors
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
     * Legacy method for partitioning a massive list of intermediate key-value pairs at once.
     * @deprecated Replaced by {@link #appendThreadSafe(List)} to support memory-safe streaming.
     */
    @Deprecated
    public void partitionAndWriteLocal(List<KeyValuePair> intermediateData) throws IOException {
        logger.warn("Invoked deprecated partitionAndWriteLocal. Use appendThreadSafe for OOM protection.");
        appendThreadSafe(intermediateData);
    }
}