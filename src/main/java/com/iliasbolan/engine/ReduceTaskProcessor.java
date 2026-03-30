package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

/**
 * Executes the Reduce phase of a Map-Reduce job utilizing Java's Fork/Join Framework
 * for optimal intra-node parallelism.
 * <p>
 * <b>Dynamic Resource Allocation:</b><br>
 * Built for cloud-native Kubernetes deployments, this processor bypasses static thread
 * assignments. It dynamically detects the container's available CPU limits via
 * {@link Runtime#availableProcessors()} to scale its processing power vertically.
 * </p>
 * <p>
 * <b>Work-Stealing Optimization:</b><br>
 * After the Shuffle phase, intermediate data is grouped by key. To process these grouped
 * records efficiently, the root task calculates a dynamic threshold based on the exact size
 * of the dataset and the available cores. This ensures the workload is split into the optimal
 * number of sub-tasks (aiming for ~15 per core), maximizing the efficiency of the
 * Fork/Join pool's work-stealing algorithm without unnecessary thread-management overhead.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.1
 * @see java.util.concurrent.RecursiveTask
 * @see java.util.concurrent.ForkJoinPool
 * @see com.iliasbolan.core.Reducer
 * @since 2026-03-30
 */
public class ReduceTaskProcessor extends RecursiveTask<List<KeyValuePair>> {

    /** The dynamically calculated maximum number of distinct keys a single thread should process sequentially. */
    private final int threshold;

    /** The grouped intermediate data, where each entry is a unique key and its list of values. */
    private final List<Map.Entry<String, List<String>>> groupedRecords;

    /** The starting index (inclusive) of the groupedRecords list assigned to this task. */
    private final int start;

    /** The ending index (exclusive) of the groupedRecords list assigned to this task. */
    private final int end;

    /** The dynamically loaded user-defined Reducer implementation. */
    private final Reducer reducer;

    /**
     * Constructs the ROOT {@code ReduceTaskProcessor} for a specific segment of grouped data.
     * <p>
     * <b>Threshold Calculation:</b> This constructor interrogates the JVM for the available
     * processor count. It then divides the total number of distinct keys by {@code (cores * 15)}
     * to calculate a target threshold. A {@link Math#max(int, int)} function guarantees the
     * threshold never drops below 50 records, preventing fragmentation on over-provisioned pods
     * handling smaller reduce partitions.
     * </p>
     *
     * @param groupedRecords The complete list of grouped records (Key -> List of Values).
     * @param start          The starting index for this task's segment (usually 0).
     * @param end            The ending index for this task's segment (usually {@code groupedRecords.size()}).
     * @param reducer        The user's {@link Reducer} implementation dynamically loaded via Reflection.
     */
    public ReduceTaskProcessor(List<Map.Entry<String, List<String>>> groupedRecords, int start, int end, Reducer reducer) {
        this.groupedRecords = groupedRecords;
        this.start = start;
        this.end = end;
        this.reducer = reducer;

        // Dynamically calculate the threshold based on total grouped records and available K8s CPU limits
        int cores = Runtime.getRuntime().availableProcessors();
        this.threshold = Math.max(50, groupedRecords.size() / (cores * 15));
    }

    /**
     * Internal constructor used exclusively for instantiating recursive sub-tasks.
     * <p>
     * <b>Performance Note:</b> By passing the pre-calculated {@code threshold} down the
     * recursion tree, we eliminate the performance penalty of querying {@code Runtime.getRuntime()}
     * and executing division math on every single fork.
     * </p>
     *
     * @param groupedRecords The complete list of grouped records.
     * @param start          The starting index for this sub-task.
     * @param end            The ending index for this sub-task.
     * @param reducer        The instantiated Reducer object.
     * @param threshold      The pre-calculated splitting threshold passed from the parent task.
     */
    private ReduceTaskProcessor(List<Map.Entry<String, List<String>>> groupedRecords, int start, int end, Reducer reducer, int threshold) {
        this.groupedRecords = groupedRecords;
        this.start = start;
        this.end = end;
        this.reducer = reducer;
        this.threshold = threshold;
    }

    /**
     * The core parallel computation method invoked by the {@link java.util.concurrent.ForkJoinPool}.
     * <p>
     * If the workload is less than or equal to the dynamically calculated {@link #threshold},
     * the grouped records are processed sequentially. Otherwise, the task is bifurcated.
     * The left half is forked asynchronously to be picked up by another core, while the
     * right half is computed immediately on the current thread.
     * </p>
     *
     * @return A consolidated {@link List} of all final {@link KeyValuePair} objects
     * generated by this task and its sub-tasks.
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
        ReduceTaskProcessor leftTask = new ReduceTaskProcessor(groupedRecords, start, middle, reducer, threshold);
        ReduceTaskProcessor rightTask = new ReduceTaskProcessor(groupedRecords, middle, end, reducer, threshold);

        // Fork the left task to run asynchronously
        leftTask.fork();

        // Compute the right task immediately on the current thread
        List<KeyValuePair> rightResult = rightTask.compute();

        // Wait for the left task to complete
        List<KeyValuePair> leftResult = leftTask.join();

        // Merge the final results
        List<KeyValuePair> mergedResult = new ArrayList<>(leftResult);
        mergedResult.addAll(rightResult);

        return mergedResult;
    }

    /**
     * Iterates through the assigned segment of grouped records and applies the user's Reduce logic.
     * <p>
     * This method represents the "leaf" nodes of the execution tree. It passes each distinct key
     * and its associated list of values to the user's custom {@link Reducer#reduce(String, List)}
     * implementation to be aggregated.
     * </p>
     *
     * @return A {@link List} of final {@link KeyValuePair} objects generated from this segment.
     */
    private List<KeyValuePair> processSequentially() {
        List<KeyValuePair> finalResults = new ArrayList<>();

        for (int i = start; i < end; i++) {
            Map.Entry<String, List<String>> entry = groupedRecords.get(i);

            // Invoke the user's custom reduce function for this key and its values
            KeyValuePair reducedResult = reducer.reduce(entry.getKey(), entry.getValue());

            if (reducedResult != null) {
                finalResults.add(reducedResult);
            }
        }

        return finalResults;
    }
}