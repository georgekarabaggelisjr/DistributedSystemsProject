package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the {@link MapTaskProcessor} utilizing a Fork/Join pool.
 */
class MapTaskProcessorTest {

    /**
     * A simple, reusable Word Count Mapper for our testing purposes.
     * It splits lines by spaces and emits (word, "1") for each word.
     */
    private final Mapper dummyWordCountMapper = (key, value) -> {
        List<KeyValuePair> results = new ArrayList<>();
        String[] words = value.split("\\s+");
        for (String word : words) {
            results.add(new KeyValuePair(word, "1"));
        }
        return results;
    };

    @Test
    void testProcessSequentially_SmallDataset_DoesNotFork() {
        // Arrange: A small dataset of 2 lines (well below the 100 threshold)
        List<String> records = List.of(
                "hello world",
                "hello george"
        );

        MapTaskProcessor processor = new MapTaskProcessor(records, 0, records.size(), dummyWordCountMapper);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act: Run the task
            List<KeyValuePair> results = pool.invoke(processor);

            // Assert: It should output exactly 4 KeyValuePairs
            assertEquals(4, results.size(), "Should have emitted 4 pairs");

            // Let's verify the actual data matches our math
            assertEquals("hello", results.get(0).key());
            assertEquals("world", results.get(1).key());
            assertEquals("hello", results.get(2).key());
            assertEquals("george", results.get(3).key());
        } finally {
            // Cleanly shut down the pool to prevent thread leaks
            pool.shutdown();
        }
    }

    @Test
    void testCompute_LargeDataset_ForcesForkJoinSplit() {
        // Arrange: We need to exceed Math.max(100) threshold to force a split.
        List<String> records = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            records.add("distributed systems"); // 2 words per line
        }

        MapTaskProcessor processor = new MapTaskProcessor(records, 0, records.size(), dummyWordCountMapper);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act: Run the task. This WILL force leftTask.fork() logic to execute
            List<KeyValuePair> results = pool.invoke(processor);

            // Assert: 300 lines * 2 words per line = 600 total emitted KeyValuePairs
            assertEquals(600, results.size(), "Fork/Join failed to aggregate all split results correctly");

            // Spot check the first and last items to ensure no data corruption happened during merging
            assertEquals("distributed", results.get(0).key());
            assertEquals("systems", results.get(599).key());
        } finally {
            // Cleanly shut down the pool
            pool.shutdown();
        }
    }
}