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
 * This ensures that the reduction logic correctly aggregates values across
 * parallel sub-tasks and maintains data integrity.
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

    @Test
    void testProcessSequentially_SmallDataset_AggregatesCorrectly() {
        // Arrange: 2 unique keys (well below the 50 threshold)
        List<Map.Entry<String, List<String>>> groupedRecords = List.of(
                new AbstractMap.SimpleEntry<>("apple", List.of("1", "1", "1")),
                new AbstractMap.SimpleEntry<>("banana", List.of("1", "1"))
        );

        ReduceTaskProcessor processor = new ReduceTaskProcessor(groupedRecords, 0, groupedRecords.size(), dummySumReducer);
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

    @Test
    void testCompute_LargeDataset_ForksAndMergesCorrectly() {
        // Arrange: Create 150 unique keys to force the Fork/Join split (threshold is 50+)
        List<Map.Entry<String, List<String>>> groupedRecords = new ArrayList<>();
        for (int i = 0; i < 150; i++) {
            groupedRecords.add(new AbstractMap.SimpleEntry<>("key_" + i, List.of("10", "20")));
        }

        ReduceTaskProcessor processor = new ReduceTaskProcessor(groupedRecords, 0, groupedRecords.size(), dummySumReducer);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act
            List<KeyValuePair> results = pool.invoke(processor);

            // Assert: Verify all 150 keys were processed and merged back together
            assertEquals(150, results.size(), "Fork/Join failed to merge all reduced results.");

            // Verify a random entry: 10 + 20 = 30
            assertEquals("30", results.get(75).value());
        } finally {
            pool.shutdown();
        }
    }
}