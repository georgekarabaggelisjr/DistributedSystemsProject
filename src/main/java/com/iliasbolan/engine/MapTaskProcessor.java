package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

/**
 * Executes the Map phase of a Map-Reduce job utilizing Java's Fork/Join Framework for
 * optimal intra-node parallelism.
 * <p>
 * <b>Dynamic Resource Allocation:</b><br>
 * Unlike traditional threaded applications with hardcoded thread counts, this processor is
 * designed for cloud-native Kubernetes environments. It dynamically detects the container's
 * available CPU limits using {@link Runtime#availableProcessors()}.
 * </p>
 * <p>
 * <b>Work-Stealing Optimization:</b><br>
 * To maximize the efficiency of the Fork/Join pool's work-stealing algorithm, the class
 * abandons a static threshold. Instead, the root task calculates a dynamic threshold based
 * on the exact size of the incoming 64MB data chunk and the number of available cores.
 * This ensures that the workload is bifurcated into an optimal number of sub-tasks
 * (approximately 15 per core), preventing both CPU under-utilization and thread-management thrashing.
 * </p>
 * <p>
 * Upon completion of the map operations, the caller is responsible for applying the
 * Shuffle partitioning logic and writing the intermediate files to the Shared File System (MinIO).
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.2
 * @see java.util.concurrent.RecursiveTask
 * @see java.util.concurrent.ForkJoinPool
 * @see com.iliasbolan.core.Mapper
 * @since 2026-03-30
 */
public class MapTaskProcessor extends RecursiveTask<List<KeyValuePair>> {

    /** * The dynamically calculated maximum number of records a single thread should process sequentially.
     * If the assigned workload exceeds this limit, the task will be split.
     */
    private final int threshold;

    /** The subset of data records (e.g., lines of a file) to be processed. */
    private final List<String> records;

    /** The starting index (inclusive) of the records list assigned to this specific task. */
    private final int start;

    /** The ending index (exclusive) of the records list assigned to this specific task. */
    private final int end;

    /** The dynamically loaded user-defined Mapper implementation. */
    private final Mapper mapper;

    /**
     * Constructs the ROOT {@code MapTaskProcessor} for a specific segment of the data chunk.
     * <p>
     * <b>Threshold Calculation:</b> This constructor interrogates the JVM for the available
     * processor count. It then divides the total number of records by {@code (cores * 15)}
     * to calculate a target threshold. A {@link Math#max(int, int)} function is applied to
     * guarantee the threshold never drops below 100 records, preventing extreme fragmentation
     * on highly over-provisioned nodes.
     * </p>
     *
     * @param records The complete list of records parsed from the downloaded chunk.
     * @param start   The starting index for this task's segment (usually 0 for the root task).
     * @param end     The ending index for this task's segment (usually {@code records.size()}).
     * @param mapper  The user's {@link Mapper} implementation dynamically loaded via Reflection.
     */
    public MapTaskProcessor(List<String> records, int start, int end, Mapper mapper) {
        this.records = records;
        this.start = start;
        this.end = end;
        this.mapper = mapper;

        // Dynamically calculate the threshold based on the total records and available K8s CPU limits
        int cores = Runtime.getRuntime().availableProcessors();
        this.threshold = Math.max(100, records.size() / (cores * 15));
    }

    /**
     * Internal constructor used exclusively for instantiating recursive sub-tasks.
     * <p>
     * <b>Performance Note:</b> By passing the pre-calculated {@code threshold} down the
     * recursion tree, we eliminate the severe performance penalty of querying
     * {@code Runtime.getRuntime()} and executing division operations on every single fork.
     * </p>
     *
     * @param records   The complete list of records.
     * @param start     The starting index for this sub-task.
     * @param end       The ending index for this sub-task.
     * @param mapper    The instantiated Mapper object.
     * @param threshold The pre-calculated splitting threshold passed from the parent task.
     */
    private MapTaskProcessor(List<String> records, int start, int end, Mapper mapper, int threshold) {
        this.records = records;
        this.start = start;
        this.end = end;
        this.mapper = mapper;
        this.threshold = threshold;
    }

    /**
     * The core parallel computation method invoked by the {@link java.util.concurrent.ForkJoinPool}.
     * <p>
     * If the assigned workload ({@code end - start}) is less than or equal to the calculated
     * {@link #threshold}, the records are processed synchronously on the current thread.
     * Otherwise, the task is bifurcated. The left half is forked asynchronously to be
     * picked up by another available core, while the right half is computed immediately.
     * </p>
     *
     * @return A consolidated {@link List} of all intermediate {@link KeyValuePair} objects
     * generated by this task and all of its underlying sub-tasks.
     */
    @Override
    protected List<KeyValuePair> compute() {
        int length = end - start;

        // Base case: workload is small enough to process sequentially
        if (length <= threshold) {
            return processSequentially();
        }

        // Recursive case: split the workload in half
        int middle = start + (length / 2);

        // Pass the pre-calculated threshold to the sub-tasks
        MapTaskProcessor leftTask = new MapTaskProcessor(records, start, middle, mapper, threshold);
        MapTaskProcessor rightTask = new MapTaskProcessor(records, middle, end, mapper, threshold);

        // Fork the left task to run asynchronously on another thread
        leftTask.fork();

        // Compute the right task immediately on the current thread
        List<KeyValuePair> rightResult = rightTask.compute();

        // Wait for the left task to complete and retrieve its result
        List<KeyValuePair> leftResult = leftTask.join();

        // Merge the intermediate results
        List<KeyValuePair> mergedResult = new ArrayList<>(leftResult);
        mergedResult.addAll(rightResult);

        return mergedResult;
    }

    /**
     * Iterates through the assigned segment of records and applies the user's Map logic.
     * <p>
     * This method represents the "leaf" nodes of the execution tree. It loops through its
     * designated index range, passing each line to the user's custom {@link Mapper#map(String, String)}
     * implementation.
     * </p>
     *
     * @return A {@link List} of intermediate {@link KeyValuePair} objects generated from this segment.
     */
    private List<KeyValuePair> processSequentially() {
        List<KeyValuePair> intermediateResults = new ArrayList<>();

        for (int i = start; i < end; i++) {
            String record = records.get(i);

            // In a standard text processing job, the key could be the line number or an offset.
            // For simplicity, we pass the line number as a String.
            String key = String.valueOf(i);

            // Invoke the user's custom map function
            List<KeyValuePair> mappedPairs = mapper.map(key, record);

            if (mappedPairs != null) {
                intermediateResults.addAll(mappedPairs);
            }
        }

        return intermediateResults;
    }
}