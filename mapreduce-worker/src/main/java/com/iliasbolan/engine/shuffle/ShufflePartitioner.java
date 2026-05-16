package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.lz4.LZ4FrameOutputStream.BLOCKSIZE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Enterprise-grade ShufflePartitioner with O(1) Memory Persistent Streams.
 * <p>
 * <b>Architectural Fix 1: Zero-Aggregation Write-Through</b><br>
 * Eliminates {@link StringBuilder} buffering that caused heap spikes during massive shuffles.
 * Records are streamed directly to partition-specific {@link BufferedWriter} instances,
 * ensuring the memory footprint remains constant regardless of chunk density or record length.
 * </p>
 * <p>
 * <b>Architectural Fix 2: Eager Directory Allocation (Skew Protection)</b><br>
 * Pre-allocates all partition directories during instantiation. This prevents fatal cache-miss
 * errors in the External Shuffle Service (ESS) overflow tier when highly skewed datasets result
 * in completely empty partitions. By guaranteeing directory existence, the ESS correctly
 * identifies empty partitions rather than triggering unnecessary and failing MinIO restores.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.3
 * @since 2026-05-15
 */
public class ShufflePartitioner implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ShufflePartitioner.class);
    private final String baseShuffleDir;
    private final String jobId;
    private final String mapTaskId;
    private final int numReducers;
    private final Object[] partitionLocks;

    /** Persistent cache of open writers to prevent LZ4 frame thrashing. */
    private final ConcurrentHashMap<Integer, BufferedWriter> writerCache = new ConcurrentHashMap<>();

    /**
     * Initializes the ShufflePartitioner and establishes the physical directory boundaries.
     *
     * @param baseShuffleDir The root physical directory for ephemeral shuffle data persistence.
     * @param jobId          The universally unique identifier (UUID) of the overarching MapReduce job.
     * @param mapTaskId      The unique identifier for the specific Map task utilizing this partitioner.
     * @param numReducers    The total number of allocated Reducers (used to define partition boundaries).
     * @throws RuntimeException If eager directory allocation fails due to disk access limits or I/O faults.
     */
    public ShufflePartitioner(String baseShuffleDir, String jobId, String mapTaskId, int numReducers) {
        this.baseShuffleDir = baseShuffleDir;
        this.jobId = jobId;
        this.mapTaskId = mapTaskId;
        this.numReducers = numReducers;

        // Lock stripping per partition to allow concurrent thread access
        this.partitionLocks = new Object[numReducers];
        for (int i = 0; i < numReducers; i++) {
            this.partitionLocks[i] = new Object();

            // EAGER DIRECTORY ALLOCATION: Data Skew / ESS Empty Partition Fix
            // Forces the creation of all partition boundaries immediately to guarantee
            // the ESS Daemon resolves empty streams correctly, bypassing erroneous S3 cache misses.
            try {
                Files.createDirectories(Paths.get(baseShuffleDir, jobId, String.valueOf(i)));
            } catch (IOException e) {
                logger.error("CRITICAL: Failed to eagerly allocate partition directory for index {}", i, e);
                throw new RuntimeException("Storage initialization failure during partition allocation", e);
            }
        }
    }

    /**
     * Appends a buffer of mapped records directly to persistent LZ4 streams.
     * <p>
     * <b>O(1) Memory Guarantee & Sequential I/O:</b> Groups the micro-batch by partition
     * in-memory first. This reduces lock acquisitions from O(N) to O(R) (where R is numReducers)
     * and guarantees sequential disk writes to prevent OS-level disk thrashing.
     * </p>
     *
     * @param buffer A transient, micro-batched collection of {@link KeyValuePair} records.
     * @throws IOException If physical disk writes or stream access operations fail.
     */
    public void appendThreadSafe(List<KeyValuePair> buffer) throws IOException {

        // 1. Group the micro-batch locally by partition to minimize lock contention
        @SuppressWarnings("unchecked")
        List<KeyValuePair>[] groupedByPartition = new List[numReducers];

        for (KeyValuePair pair : buffer) {
            int partitionIndex = (pair.key().hashCode() & Integer.MAX_VALUE) % numReducers;

            if (groupedByPartition[partitionIndex] == null) {
                // Initialize with a small capacity to prevent array reallocation overhead
                groupedByPartition[partitionIndex] = new ArrayList<>(buffer.size() / numReducers + 10);
            }
            groupedByPartition[partitionIndex].add(pair);
        }

        // 2. Flush to disk: Acquire the lock ONCE per active partition
        for (int i = 0; i < numReducers; i++) {
            List<KeyValuePair> partitionRecords = groupedByPartition[i];

            if (partitionRecords != null && !partitionRecords.isEmpty()) {
                // ONE lock acquisition for the entire sub-batch destined for this file
                synchronized (partitionLocks[i]) {
                    BufferedWriter writer = getOrCreateWriter(i);

                    // Sequential write to the OS buffer
                    for (KeyValuePair pair : partitionRecords) {
                        writer.write(pair.key());
                        writer.write('\t');
                        writer.write(pair.value());
                        writer.write('\n');
                    }
                }
            }
        }
    }

    /**
     * Retrieves a cached writer or dynamically provisions a new LZ4 stream for the partition.
     *
     * @param partitionIndex The mathematical routing destination for the data payload.
     * @return A ready-to-write, buffered, LZ4-compressed output stream.
     * @throws IOException If the file cannot be created or opened for I/O operations.
     */
    private BufferedWriter getOrCreateWriter(int partitionIndex) throws IOException {
        BufferedWriter cachedWriter = writerCache.get(partitionIndex);
        if (cachedWriter != null) return cachedWriter;

        Path partitionDir = Paths.get(baseShuffleDir, jobId, String.valueOf(partitionIndex));

        // Note: Directory is guaranteed to exist due to eager allocation in the constructor,
        // but retaining this call ensures idempotency if manual disk interventions occurred.
        Files.createDirectories(partitionDir);

        // Use TRUNCATE_EXISTING instead of APPEND. If a Map task is retried (Lineage Recovery),
        // the system must start a fresh LZ4 frame to prevent binary corruption on the Reducer.
        Path filePath = partitionDir.resolve(mapTaskId + ".lz4");
        OutputStream os = Files.newOutputStream(filePath,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(os, BLOCKSIZE.SIZE_4MB);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(lz4os, StandardCharsets.UTF_8));

        writerCache.put(partitionIndex, writer);
        return writer;
    }

    /**
     * Safely flushes and terminates all active LZ4 streams managed by this partitioner.
     * <p>
     * Essential for ensuring that all compressed frames are fully finalized on the local disk
     * before the TaskExecutor signals a successful completion to the Orchestrator.
     * </p>
     */
    @Override
    public void close() {
        logger.info("Finalizing LZ4 partition frames for task: {}", mapTaskId);
        for (BufferedWriter writer : writerCache.values()) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                logger.error("Failed to finalize shuffle stream for task: {}", mapTaskId, e);
            }
        }
        writerCache.clear();
    }
}