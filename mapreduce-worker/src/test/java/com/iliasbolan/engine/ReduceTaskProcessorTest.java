package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Reducer;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the {@link ReduceTaskProcessor} utilizing a Fork/Join pool.
 * <p>
 * <b>Spill-to-Disk Architecture Update:</b><br>
 * This suite has been updated to reflect the processor's new role as a high-speed
 * Batch Processor. It verifies that constrained memory batches (streamed from the
 * ExternalMergeSorter) are correctly distributed across available CPU cores
 * and accurately merged without data loss.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 */
class ReduceTaskProcessorTest {

    /**
     * A simple Summing Reducer for testing.
     * It takes a list of numeric strings and returns their sum as a string.
     */
    private final Reducer dummySumReducer = (key, values) -> {
        int sum = values.stream()
                .mapToInt(Integer::parseInt)
                .sum();
        return new KeyValuePair(key, String.valueOf(sum));
    };

    /**
     * Verifies that small batches bypass Fork/Join bifurcation and execute
     * sequentially on a single thread.
     *
     * """
     * Validates the sequential processing logic for sub-threshold batch sizes.
     * """
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
     * Verifies that large batches dynamically trigger the Fork/Join split,
     * executing recursively across multiple threads.
     *
     * """
     * Validates the work-stealing division and merging mechanics.
     * * Proves that splitting a batch does not drop keys during the
     * post-execution merge process.
     * """
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