package com.iliasbolan.engine.execution;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

/**
 * Executes the Reduce phase for a bounded batch of Map-Reduce data utilizing
 * Java's Fork/Join Framework for optimal intra-node parallelism.
 * <p>
 * <b>Dynamic Parallelism Model:</b><br>
 * This processor implements a container-aware resource discovery pattern. It utilizes the
 * {@link Runtime#availableProcessors()} method—which in Java 17 accurately reflects
 * Kubernetes CPU limits—and applies a configurable <code>PARALLELISM_FACTOR</code>.
 * This allows the worker to over-provision virtual threads to mask I/O latency
 * during disk-based merge-sort operations.
 * </p>
 * <p>
 * <b>Work-Stealing Optimization:</b><br>
 * The split threshold is calculated dynamically to ensure a high fan-out ratio
 * (roughly 10-12 tasks per target thread). This maximizes the efficiency of the
 * {@link java.util.concurrent.ForkJoinPool} work-stealing algorithm, preventing
 * thread starvation in multitenant clusters.
 * </p>
 * <p>
 * <i>Architectural Note:</i> While this class provides excellent intra-node parallelism
 * for memory-bounded datasets, workloads processing massive, highly skewed datasets
 * (e.g., Zipfian distributions) should prefer lazy-evaluated disk streaming
 * (e.g., via iterators) to guarantee O(1) memory safety.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.2
 * @since 2026-04-26
 */
public class ReduceTaskProcessor extends RecursiveTask<List<KeyValuePair>> {

    private static final Logger logger = LoggerFactory.getLogger(ReduceTaskProcessor.class);

    /** * The dynamically calculated maximum number of distinct keys a single thread
     * should process sequentially before splitting the task.
     */
    private final int threshold;

    /** * The grouped intermediate data batch, where each entry represents a
     * unique key and its associated list of values.
     */
    private final List<Map.Entry<String, List<String>>> groupedBatch;

    /** The starting index (inclusive) of the segment in the groupedBatch. */
    private final int start;

    /** The ending index (exclusive) of the segment in the groupedBatch. */
    private final int end;

    /** The dynamically loaded user-defined Reducer implementation. */
    private final Reducer reducer;

    /**
     * Constructs a root {@code ReduceTaskProcessor} with dynamic resource discovery.
     * <p>
     * Detects Kubernetes vCPU quotas and applies a scaling factor (default 2.0)
     * to determine the target thread count. It then establishes a task-split
     * threshold tailored to the current batch size.
     * </p>
     *
     * @param groupedBatch A memory-bounded list of grouped records (Key mapped to Values).
     * @param start        The starting index (inclusive) for this task's segment.
     * @param end          The ending index (exclusive) for this task's segment.
     * @param reducer      The user-provided implementation of the {@link Reducer} interface.
     */
    public ReduceTaskProcessor(List<Map.Entry<String, List<String>>> groupedBatch, int start, int end, Reducer reducer) {
        this.groupedBatch = groupedBatch;
        this.start = start;
        this.end = end;
        this.reducer = reducer;

        // --- DYNAMIC PARALLELISM DISCOVERY ---
        // 1. Detect K8s vCPU quota (Java 17 is container-aware)
        int vCpus = Runtime.getRuntime().availableProcessors();

        // 2. Load the scaling factor from environment variables
        // Defaulting to 2.0 to account for External Sorter disk I/O wait times
        double factor = Double.parseDouble(
                System.getenv().getOrDefault("PARALLELISM_FACTOR", "2.0")
        );
        int targetThreads = (int) Math.ceil(vCpus * factor);

        // 3. Calculate Threshold (Aim for ~10-12 tasks per target thread to enable work-stealing)
        this.threshold = Math.max(10, groupedBatch.size() / (targetThreads * 10));

        logger.info("Dynamic Parallelism Discovery (Reduce): [vCPUs: {}, Factor: {}, Target Threads: {}, Threshold: {}]",
                vCpus, factor, targetThreads, this.threshold);
    }

    /**
     * Internal constructor for instantiating recursive sub-tasks.
     *
     * @param groupedBatch The data batch shared across tasks.
     * @param start        Starting index (inclusive) of the sub-segment.
     * @param end          Ending index (exclusive) of the sub-segment.
     * @param reducer      The Reducer implementation.
     * @param threshold    The pre-calculated split threshold.
     */
    private ReduceTaskProcessor(List<Map.Entry<String, List<String>>> groupedBatch, int start, int end, Reducer reducer, int threshold) {
        this.groupedBatch = groupedBatch;
        this.start = start;
        this.end = end;
        this.reducer = reducer;
        this.threshold = threshold;
    }

    @Override
    protected List<KeyValuePair> compute() {
        int length = end - start;

        // Base case: workload is small enough to process sequentially
        if (length <= threshold) {
            return processSequentially();
        }

        // Recursive case: split the workload in half
        int middle = start + (length / 2);

        ReduceTaskProcessor leftTask = new ReduceTaskProcessor(groupedBatch, start, middle, reducer, threshold);
        ReduceTaskProcessor rightTask = new ReduceTaskProcessor(groupedBatch, middle, end, reducer, threshold);

        // Fork the left task for asynchronous execution
        leftTask.fork();

        // Compute the right task immediately on the current thread
        List<KeyValuePair> rightResult = rightTask.compute();

        // Wait for the left task and merge the results
        List<KeyValuePair> leftResult = leftTask.join();

        // PERFORMANCE OPTIMIZATION: Pre-allocate merged list capacity
        // Prevents array reallocation when adding the right tree results
        List<KeyValuePair> mergedResult = new ArrayList<>(leftResult.size() + rightResult.size());
        mergedResult.addAll(leftResult);
        mergedResult.addAll(rightResult);

        return mergedResult;
    }

    /**
     * Sequentially processes a segment of the batch using the Reducer logic.
     *
     * @return A {@link List} of {@link KeyValuePair} objects generated by
     * applying the Reduce logic to this specific segment.
     */
    private List<KeyValuePair> processSequentially() {
        // PERFORMANCE OPTIMIZATION: Pre-allocate to maximum possible size
        List<KeyValuePair> finalResults = new ArrayList<>(end - start);

        for (int i = start; i < end; i++) {
            Map.Entry<String, List<String>> entry = groupedBatch.get(i);

            // Execute user-defined reduction logic
            KeyValuePair reducedResult = reducer.reduce(entry.getKey(), entry.getValue().iterator());

            if (reducedResult != null) {
                finalResults.add(reducedResult);
            }
        }

        return finalResults;
    }
}