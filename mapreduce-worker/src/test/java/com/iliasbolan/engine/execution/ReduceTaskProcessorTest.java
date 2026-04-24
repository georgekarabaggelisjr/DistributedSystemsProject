package com.iliasbolan.engine.execution;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test suite for the {@link ReduceTaskProcessor} utilizing the Fork/Join framework.
 *
 * <p><b>Architectural Context: Spill-to-Disk Batch Processing</b><br>
 * This suite validates the processor's capability to function as a high-speed batch
 * execution engine. It specifically verifies that memory-constrained batches—streamed
 * from the {@link ExternalMergeSorter}—are correctly partitioned across available
 * CPU cores and deterministically merged without data loss or corruption.</p>
 *
 * <p>The tests focus on two primary execution paths:
 * <ul>
 * <li><b>Sequential Base Case:</b> Small batches that fall below the dynamic splitting threshold.</li>
 * <li><b>Recursive Parallel Case:</b> Large batches that trigger work-stealing bifurcation
 * across the {@link ForkJoinPool}.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @see ReduceTaskProcessor
 * @see java.util.concurrent.ForkJoinPool
 */
class ReduceTaskProcessorTest {

    /**
     * A functional {@link Reducer} implementation used for verification purposes.
     * Sums a list of numeric strings and returns the result as a {@link KeyValuePair}.
     */
    private final Reducer dummySumReducer = (key, values) -> {
        int sum = values.stream()
                .mapToInt(Integer::parseInt)
                .sum();
        return new KeyValuePair(key, String.valueOf(sum));
    };

    /**
     * Verifies that batches smaller than the dynamic threshold bypass the Fork/Join
     * bifurcation logic.
     *
     * <p>This test ensures that sequential processing remains accurate for small datasets
     * and that the system handles the terminal base-case of the recursion correctly.</p>
     */
    @Test
    void testProcessSequentially_SmallBatch_AggregatesCorrectly() {
        // Arrange: 2 unique keys (well below the new minimum threshold of 10)
        List<Map.Entry<String, List<String>>> groupedBatch = List.of(
                new AbstractMap.SimpleEntry<>("apple", List.of("1", "1", "1")),
                new AbstractMap.SimpleEntry<>("banana", List.of("1", "1"))
        );

        ReduceTaskProcessor processor = new ReduceTaskProcessor(groupedBatch, 0, groupedBatch.size(), dummySumReducer);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act
            List<KeyValuePair> results = pool.invoke(processor);

            // Assert
            assertEquals(2, results.size());

            // Check apple sum: 1+1+1 = 3
            assertEquals("apple", results.get(0).key());
            assertEquals("3", results.get(0).value());

            // Check banana sum: 1+1 = 2
            assertEquals("banana", results.get(1).key());
            assertEquals("2", results.get(1).value());
        } finally {
            pool.shutdown();
        }
    }

    /**
     * Verifies that large data batches dynamically trigger the Fork/Join recursive
     * split mechanics.
     *
     * <p>This test validates the work-stealing division and the subsequent merging
     * of result sets across multiple threads, proving that the recursive splitting
     * does not drop keys during the post-execution aggregation phase.</p>
     */
    @Test
    void testCompute_LargeBatch_ForksAndMergesCorrectly() {
        // Arrange: Create 150 unique keys to force the Fork/Join split (threshold is 10+ for memory-safe batches)
        List<Map.Entry<String, List<String>>> groupedBatch = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            groupedBatch.add(new AbstractMap.SimpleEntry<>("key_" + i, List.of("10", "20")));
        }

        ReduceTaskProcessor processor = new ReduceTaskProcessor(groupedBatch, 0, groupedBatch.size(), dummySumReducer);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act
            List<KeyValuePair> results = pool.invoke(processor);

            // Assert: Verify all 150 keys were processed and merged back together
            assertEquals(150, results.size(), "Fork/Join failed to merge all reduced batch results.");

            // Verify a random entry: 10 + 20 = 30
            assertEquals("30", results.get(75).value());
        } finally {
            pool.shutdown();
        }
    }
}