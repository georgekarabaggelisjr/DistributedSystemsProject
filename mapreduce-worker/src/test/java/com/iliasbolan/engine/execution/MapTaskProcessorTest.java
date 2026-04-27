package com.iliasbolan.engine.execution;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.engine.shuffle.ShufflePartitioner;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
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
 * @version 3.0
 */
class MapTaskProcessorTest {

    /**
     * A simple, reusable Word Count Mapper for our testing purposes.
     * It splits lines by spaces and emits words directly to the Context.
     * Updated to comply with the Streaming Context Pattern.
     */
    private final Mapper dummyWordCountMapper = (value, context) -> {
        // Replace invalid characters with a space instead of deleting them to prevent empty outputs
        String cleanValue = value.replaceAll("[^a-zA-Z0-9 ]", " ");
        String[] words = cleanValue.split("\\s+");
        for (String word : words) {
            if (!word.trim().isEmpty()) {
                context.write(word, "1"); // Stream directly to context!
            }
        }
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

        // We use a synchronized list to safely capture flushed data before buffer.clear() is called
        List<KeyValuePair> interceptedResults = Collections.synchronizedList(new ArrayList<>());
        doAnswer(invocation -> {
            List<KeyValuePair> flushedBuffer = invocation.getArgument(0);
            interceptedResults.addAll(new ArrayList<>(flushedBuffer)); // Deep copy before clearance
            return null;
        }).when(mockPartitioner).appendThreadSafe(anyList());

        MapTaskProcessor processor = new MapTaskProcessor(records, 0, records.size(), dummyWordCountMapper, mockPartitioner);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act: Run the void task
            pool.invoke(processor);

            // Assert: Verify the partitioner was called
            verify(mockPartitioner, times(1)).appendThreadSafe(anyList());

            // It should output exactly 4 KeyValuePairs
            assertEquals(4, interceptedResults.size(), "Should have emitted exactly 4 pairs to the partitioner.");

            // Verify the actual data matches our math
            assertEquals("hello", interceptedResults.get(0).key());
            assertEquals("world", interceptedResults.get(1).key());
            assertEquals("hello", interceptedResults.get(2).key());
            assertEquals("george", interceptedResults.get(3).key());
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

        // Use a synchronized list to safely aggregate results across all Fork/Join threads
        List<KeyValuePair> allInterceptedResults = Collections.synchronizedList(new ArrayList<>());
        doAnswer(invocation -> {
            List<KeyValuePair> flushedBuffer = invocation.getArgument(0);
            allInterceptedResults.addAll(new ArrayList<>(flushedBuffer)); // Deep copy
            return null;
        }).when(mockPartitioner).appendThreadSafe(anyList());

        MapTaskProcessor processor = new MapTaskProcessor(records, 0, records.size(), dummyWordCountMapper, mockPartitioner);
        ForkJoinPool pool = new ForkJoinPool();

        try {
            // Act: Run the void task. This WILL force leftTask.fork() logic to execute
            pool.invoke(processor);

            // Assert: Intercept all concurrent flushes across all Fork/Join threads
            verify(mockPartitioner, atLeastOnce()).appendThreadSafe(anyList());

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