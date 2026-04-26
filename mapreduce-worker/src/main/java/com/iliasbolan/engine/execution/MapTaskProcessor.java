package com.iliasbolan.engine.execution;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.engine.shuffle.ShufflePartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Executes the Map phase of a Map-Reduce job utilizing Java's Fork/Join Framework for
 * optimal intra-node parallelism.
 * <p>
 * <b>Spill-to-Disk Memory Optimization:</b><br>
 * Upgraded to a {@link RecursiveAction} to completely bypass in-memory list aggregation.
 * Instead of merging millions of intermediate records into a single master list, each
 * thread processes a small subset of the data chunk and immediately delegates the
 * results to a thread-safe {@link ShufflePartitioner}. This guarantees a flat memory
 * footprint regardless of chunk density.
 * </p>
 * <p>
 * <b>Dynamic Resource Allocation:</b><br>
 * Unlike traditional threaded applications with hardcoded thread counts, this processor is
 * designed for cloud-native Kubernetes environments. It dynamically detects the container's
 * available CPU limits using {@link Runtime#availableProcessors()}.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @see java.util.concurrent.RecursiveAction
 * @see java.util.concurrent.ForkJoinPool
 * @see com.iliasbolan.core.Mapper
 * @since 2026-04-24
 */
public class MapTaskProcessor extends RecursiveAction {

    private static final Logger logger = LoggerFactory.getLogger(MapTaskProcessor.class);

    /** The dynamically calculated maximum number of records a single thread should process sequentially. */
    private final int threshold;

    /** The subset of data records (e.g., lines of a file) to be processed. */
    private final List<String> records;

    /** The starting index (inclusive) of the records list assigned to this specific task. */
    private final int start;

    /** The ending index (exclusive) of the records list assigned to this specific task. */
    private final int end;

    /** The dynamically loaded user-defined Mapper implementation. */
    private final Mapper mapper;

    /** The thread-safe partitioner responsible for spilling mapped data directly to local disk. */
    private final ShufflePartitioner partitioner;

    /**
     * Constructs the ROOT {@code MapTaskProcessor} for a specific segment of the data chunk.
     * <p>
     * Initializes the root Fork/Join action for mapping records and spilling to disk.
     * Calculates a dynamic split threshold based on available CPU cores to ensure
     * proper fan-out. Binds a shared Partitioner to handle concurrent disk writes.
     * </p>
     *
     * @param records     The complete list of UTF-8 records parsed from the chunk.
     * @param start       The starting index for this task's segment.
     * @param end         The ending index for this task's segment.
     * @param mapper      The user's dynamic Mapper implementation.
     * @param partitioner The service handling concurrent disk spills.
     */
    public MapTaskProcessor(List<String> records, int start, int end, Mapper mapper, ShufflePartitioner partitioner) {
        this.records = records;
        this.start = start;
        this.end = end;
        this.mapper = mapper;
        this.partitioner = partitioner;

        // 1. Detect K8s CPU quota (Container Aware)
        int vCpus = Runtime.getRuntime().availableProcessors();

        // 2. Load Scaling Factor from environment (Allows 2.0x for I/O masking)
        double factor = Double.parseDouble(System.getenv().getOrDefault("PARALLELISM_FACTOR", "2.0"));
        int targetThreads = (int) Math.ceil(vCpus * factor);

        // 3. Calculate Threshold to ensure enough tasks exist for work-stealing
        // Aiming for ~12 tasks per target thread to keep the ForkJoinPool saturated.
        this.threshold = Math.max(100, records.size() / (targetThreads * 12));

        logger.info("Dynamic Parallelism discovery: [vCPUs: {}, Factor: {}, Target Threads: {}, Threshold: {}]",
                vCpus, factor, targetThreads, this.threshold);
    }

    /**
     * Internal constructor used exclusively for instantiating recursive sub-tasks.
     * <p>
     * Bypasses the heavy dynamic threshold calculation for sub-tasks to maximize performance.
     * </p>
     *
     * @param records     The complete list of UTF-8 records parsed from the chunk.
     * @param start       The starting index for this task's segment.
     * @param end         The ending index for this task's segment.
     * @param mapper      The user's dynamic Mapper implementation.
     * @param partitioner The service handling concurrent disk spills.
     * @param threshold   The calculated maximum number of records a single thread should process sequentially.
     */
    private MapTaskProcessor(List<String> records, int start, int end, Mapper mapper, ShufflePartitioner partitioner, int threshold) {
        this.records = records;
        this.start = start;
        this.end = end;
        this.mapper = mapper;
        this.partitioner = partitioner;
        this.threshold = threshold;
    }

    /**
     * The core parallel computation method invoked by the {@link java.util.concurrent.ForkJoinPool}.
     * <p>
     * Recursively splits the workload in half until the segment size falls below the threshold.
     * Because this is a RecursiveAction, no data is merged or returned. Threads execute
     * their batches and flush to disk independently.
     * </p>
     */
    @Override
    protected void compute() {
        int length = end - start;

        // Base case: workload is small enough to process sequentially
        if (length <= threshold) {
            processSequentially();
            return;
        }

        // Recursive case: split the workload in half
        int middle = start + (length / 2);

        MapTaskProcessor leftTask = new MapTaskProcessor(records, start, middle, mapper, partitioner, threshold);
        MapTaskProcessor rightTask = new MapTaskProcessor(records, middle, end, mapper, partitioner, threshold);

        // Fork the left task to run asynchronously on another thread
        leftTask.fork();

        // Compute the right task immediately on the current thread
        rightTask.compute();

        // Wait for the left task to complete
        leftTask.join();
    }

    /**
     * Iterates through the assigned segment of records, applies the Map logic, and flushes to disk.
     * <p>
     * The leaf-node execution logic. Passes lines to the user-defined {@code map()} function
     * and immediately flushes the intermediate results to the thread-safe Partitioner.
     * </p>
     *
     * @throws RuntimeException If the disk spill operation fails.
     */
    private void processSequentially() {
        List<KeyValuePair> localBuffer = new ArrayList<>();

        for (int i = start; i < end; i++) {
            String record = records.get(i);
            String key = String.valueOf(i);

            List<KeyValuePair> mappedPairs = mapper.map(key, record);

            if (mappedPairs != null) {
                localBuffer.addAll(mappedPairs);
            }
        }

        // Flush directly to disk via the partitioner, completely bypassing root list aggregation
        if (!localBuffer.isEmpty()) {
            try {
                partitioner.appendThreadSafe(localBuffer);
            } catch (Exception e) {
                logger.error("Failed to spill mapped records to disk", e);
                throw new RuntimeException("Disk spill failed during Map phase", e);
            }
        }
    }
}