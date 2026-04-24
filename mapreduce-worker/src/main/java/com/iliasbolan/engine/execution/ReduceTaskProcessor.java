package com.iliasbolan.engine.execution;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RecursiveTask;

/**
 * Executes the Reduce phase for a bounded batch of Map-Reduce data utilizing
 * Java's Fork/Join Framework for optimal intra-node parallelism.
 *
 * <p><b>Architectural Evolution: Spill-to-Disk Integration</b><br>
 * In legacy versions, this processor attempted to load entire datasets into memory,
 * leading to potential {@link OutOfMemoryError} states. Under the current external merge
 * sort pipeline, this class operates as a high-speed batch processor. The
 * {@link ExternalMergeSorter} streams sorted data from disk, groups a manageable
 * number of distinct keys, and dispatches them to this task for parallel reduction.</p>
 *
 * <p><b>Work-Stealing and Resource Efficiency</b><br>
 * This implementation is optimized for containerized environments (Kubernetes).
 * It dynamically calculates a processing threshold based on available CPU quotas
 * and batch size, ensuring that the {@link java.util.concurrent.ForkJoinPool}
 * work-stealing algorithm is utilized effectively without causing thread starvation.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 * @see java.util.concurrent.RecursiveTask
 * @see java.util.concurrent.ForkJoinPool
 * @see com.iliasbolan.core.Reducer
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
     * Constructs a root {@code ReduceTaskProcessor} to handle a specific
     * memory-bounded batch of grouped data.
     *
     * <p>This constructor initializes the primary Fork/Join task and calculates
     * a dynamic split threshold based on active CPU cores to ensure effective
     * fan-out, even for constrained batch sizes.</p>
     *
     * @param groupedBatch A memory-bounded list of grouped records (Key mapped to Values).
     * @param start        The starting index for this task's segment.
     * @param end          The ending index for this task's segment.
     * @param reducer      The user-provided implementation of the {@link Reducer} interface.
     */
    public ReduceTaskProcessor(List<Map.Entry<String, List<String>>> groupedBatch, int start, int end, Reducer reducer) {
        this.groupedBatch = groupedBatch;
        this.start = start;
        this.end = end;
        this.reducer = reducer;

        // Dynamically calculate the threshold based on batch size and available K8s CPU limits.
        // Minimum threshold of 10 ensures small batches still distribute across cores.
        int cores = Runtime.getRuntime().availableProcessors();
        this.threshold = Math.max(10, groupedBatch.size() / (cores * 5));

        logger.debug("Initialized ROOT ReduceTaskProcessor for Batch. Unique Keys: {}, Cores: {}, Threshold: {}",
                groupedBatch.size(), cores, this.threshold);
    }

    /**
     * Internal constructor for instantiating recursive sub-tasks.
     *
     * <p>Bypasses dynamic threshold recalculation to minimize overhead
     * during the task-splitting process.</p>
     *
     * @param groupedBatch The data batch shared across tasks.
     * @param start        Starting index of the sub-segment.
     * @param end          Ending index of the sub-segment.
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

    /**
     * Orchestrates the parallel computation via a divide-and-conquer strategy.
     *
     * <p>Recursively partitions the workload until the segment size is below
     * the defined {@code threshold}. Sub-tasks are forked into the
     * {@link java.util.concurrent.ForkJoinPool}, and results are merged
     * as the recursion unwinds.</p>
     *
     * @return A consolidated {@link List} of {@link KeyValuePair} objects
     * representing the finalized reduced data for this task tree.
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

        ReduceTaskProcessor leftTask = new ReduceTaskProcessor(groupedBatch, start, middle, reducer, threshold);
        ReduceTaskProcessor rightTask = new ReduceTaskProcessor(groupedBatch, middle, end, reducer, threshold);

        // Fork the left task to run asynchronously
        leftTask.fork();

        // Compute the right task immediately on the current thread
        List<KeyValuePair> rightResult = rightTask.compute();

        // Wait for the left task to complete and join results
        List<KeyValuePair> leftResult = leftTask.join();

        List<KeyValuePair> mergedResult = new ArrayList<>(leftResult);
        mergedResult.addAll(rightResult);

        return mergedResult;
    }

    /**
     * Sequentially processes a segment of the batch using the Reducer logic.
     *
     * <p>This method represents the leaf-node execution where the user-defined
     * {@code reduce()} function is applied to distinct keys and their associated
     * value collections.</p>
     *
     * @return A {@link List} of {@link KeyValuePair} objects generated by
     * applying the Reduce logic to this specific segment.
     */
    private List<KeyValuePair> processSequentially() {
        List<KeyValuePair> finalResults = new ArrayList<>();

        for (int i = start; i < end; i++) {
            Map.Entry<String, List<String>> entry = groupedBatch.get(i);

            // Invoke the user's custom reduce function
            KeyValuePair reducedResult = reducer.reduce(entry.getKey(), entry.getValue());

            if (reducedResult != null) {
                finalResults.add(reducedResult);
            }
        }

        return finalResults;
    }
}