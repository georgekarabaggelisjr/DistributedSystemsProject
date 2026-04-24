package com.iliasbolan.engine.execution;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.engine.shuffle.ShufflePartitioner;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link MapTaskProcessor} utilizing a Fork/Join pool.
 * <p>
 * <b>Spill-to-Disk Architecture Update:</b><br>
 * This suite has been heavily refactored to test the new {@link java.util.concurrent.RecursiveAction}
 * void-return model. Because the processor no longer returns a consolidated master list,
 * these tests utilize Mockito to intercept and validate the memory-safe batches
 * being streamed directly to the {@link ShufflePartitioner}.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 */
class MapTaskProcessorTest {

    /**
     * A simple, reusable Word Count Mapper for our testing purposes.
     * It splits lines by spaces and emits (word, "1") for each word.
     */
    private final Mapper dummyWordCountMapper = (key, value) -> {
        List<KeyValuePair> results = new ArrayList<>();
        // Replace invalid characters with a space instead of deleting them to prevent empty outputs
        String cleanValue = value.replaceAll("[^a-zA-Z0-9 ]", " ");
        String[] words = cleanValue.split("\\s+");
        for (String word : words) {
            if (!word.trim().isEmpty()) {
                results.add(new KeyValuePair(word, "1"));
            }
        }
        return results;
    };

    /**
     * Verifies that small datasets bypass Fork/Join bifurcation and stream
     * directly to the partitioner in a single synchronous operation.
     *
     * @throws Exception if the mocked partitioner throws an I/O exception.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testProcessSequentially_SmallDataset_DoesNotFork() throws Exception {
        // Arrange: A small dataset of 2 lines (well below the 100 threshold)
        List<String> records = List.of(
                "hello world",
                "hello george"
        );

        ShufflePartitioner mockPartitioner = mock(ShufflePartitioner.class);
        MapTaskProcessor processor = new MapTaskProcessor(records, 0, records.size(), dummyWordCountMapper, mockPartitioner);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act: Run the void task
            pool.invoke(processor);

            // Assert: Intercept the flush to the partitioner
            ArgumentCaptor<List<KeyValuePair>> captor = ArgumentCaptor.forClass(List.class);
            verify(mockPartitioner, times(1)).appendThreadSafe(captor.capture());

            List<KeyValuePair> results = captor.getValue();

            // It should output exactly 4 KeyValuePairs
            assertEquals(4, results.size(), "Should have emitted exactly 4 pairs to the partitioner.");

            // Verify the actual data matches our math
            assertEquals("hello", results.get(0).key());
            assertEquals("world", results.get(1).key());
            assertEquals("hello", results.get(2).key());
            assertEquals("george", results.get(3).key());
        } finally {
            // Cleanly shut down the pool to prevent thread leaks
            pool.shutdown();
        }
    }

    /**
     * Verifies that large datasets accurately trigger Fork/Join splits, resulting
     * in multiple concurrent flushes to the partitioner without dropping records.
     *
     * @throws Exception if the mocked partitioner throws an I/O exception.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testCompute_LargeDataset_ForcesForkJoinSplit() throws Exception {
        // Arrange: We need to exceed Math.max(100) threshold to force a split.
        List<String> records = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            records.add("distributed systems"); // 2 words per line
        }

        ShufflePartitioner mockPartitioner = mock(ShufflePartitioner.class);
        MapTaskProcessor processor = new MapTaskProcessor(records, 0, records.size(), dummyWordCountMapper, mockPartitioner);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act: Run the void task. This WILL force leftTask.fork() logic to execute
            pool.invoke(processor);

            // Assert: Intercept all concurrent flushes across all Fork/Join threads
            ArgumentCaptor<List<KeyValuePair>> captor = ArgumentCaptor.forClass(List.class);
            verify(mockPartitioner, atLeastOnce()).appendThreadSafe(captor.capture());

            // Aggregate intercepted batches to prove mathematical correctness
            List<KeyValuePair> allInterceptedResults = new ArrayList<>();
            for (List<KeyValuePair> batch : captor.getAllValues()) {
                allInterceptedResults.addAll(batch);
            }

            // 300 lines * 2 words per line = 600 total emitted KeyValuePairs
            assertEquals(600, allInterceptedResults.size(), "Fork/Join failed to flush all split results correctly");

            // Since flushing is highly concurrent, order is non-deterministic.
            // We verify correctness by counting the exact occurrences instead of checking specific indices.
            long distributedCount = allInterceptedResults.stream().filter(p -> "distributed".equals(p.key())).count();
            long systemsCount = allInterceptedResults.stream().filter(p -> "systems".equals(p.key())).count();

            assertEquals(300, distributedCount, "Dropped records for key 'distributed'.");
            assertEquals(300, systemsCount, "Dropped records for key 'systems'.");

        } finally {
            // Cleanly shut down the pool
            pool.shutdown();
        }
    }
}