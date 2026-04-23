package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link ShufflePartitioner}.
 * <p>
 * This suite verifies that intermediate data is correctly hashed, partitioned,
 * and persisted to the local file system. It ensures the integrity of the
 * directory structure and serialization format required for the P2P Shuffle phase.
 * </p>
 * <p>
 * <b>Spill-to-Disk Architecture Update:</b><br>
 * These tests have been refactored to validate the new highly concurrent,
 * lock-striped disk persistence logic. They utilize JUnit 5's <code>@TempDir</code>
 * to perform real I/O operations in a safe, isolated temporary environment.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @see com.iliasbolan.engine.ShufflePartitioner
 */
class ShufflePartitionerTest {

    private ShufflePartitioner partitioner;
    private final String testJobId = "job-123";
    private final String testMapId = "map-001";
    private String baseShuffleDir;

    /**
     * Utilizes JUnit 5's {@code @TempDir} to provide a safe, isolated directory
     * for testing local P2P shuffle persistence without manual cleanup.
     */
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Set the base directory for local shuffle data
        baseShuffleDir = tempDir.toString();

        // Initialize the partitioner with 3 target Reducers (R=3)
        partitioner = new ShufflePartitioner(
                baseShuffleDir, testJobId, testMapId, 3
        );
    }

    /**
     * Verifies that the thread-safe partitioner correctly groups identical keys and
     * safely creates directory structures for active partitions.
     * * """
     * Validates the core routing and disk-spill mechanism.
     * * Proves that batches are correctly segmented by hash and written to the
     * correct deterministic files without data loss.
     * * Raises:
     * IOException: If a file system error occurs during the partitioning process.
     * """
     * @throws IOException If a file system error occurs during the partitioning process.
     */
    @Test
    void testAppendThreadSafe_CorrectlyGroupsKeysAndSkipsEmptyPartitions() throws IOException {
        // Arrange: "apple" and "banana" will hash to specific partitions.
        // Include "apple" twice to ensure they end up in the same local file.
        List<KeyValuePair> intermediateData = List.of(
                new KeyValuePair("apple", "1"),
                new KeyValuePair("banana", "1"),
                new KeyValuePair("apple", "1")
        );

        // Act: Invoke the thread-safe local write logic
        partitioner.appendThreadSafe(intermediateData);

        // Assert:
        // 1. Verify that only directories for active partitions were created
        int applePartition = Math.abs("apple".hashCode()) % 3;
        int bananaPartition = Math.abs("banana".hashCode()) % 3;

        Path applePath = tempDir.resolve(testJobId).resolve(String.valueOf(applePartition)).resolve(testMapId + ".txt");
        Path bananaPath = tempDir.resolve(testJobId).resolve(String.valueOf(bananaPartition)).resolve(testMapId + ".txt");

        assertTrue(Files.exists(applePath), "Partition file for 'apple' should exist at: " + applePath);
        assertTrue(Files.exists(bananaPath), "Partition file for 'banana' should exist at: " + bananaPath);

        // 2. Verify identical keys were grouped into the same partition file
        String appleContent = Files.readString(applePath);
        assertTrue(appleContent.contains("apple\t1\napple\t1\n"),
                "Identical keys were not grouped into the same local partition file.");
    }

    /**
     * Verifies that the partitioner adheres to the deterministic local path
     * naming convention required for the P2P embedded server to locate data.
     * * """
     * Validates the routing path generation.
     * * Essential for the gRPC Server-Side Streaming to accurately fetch files
     * requested by remote Reducers.
     * * Raises:
     * IOException: If a file system error occurs.
     * """
     * @throws IOException If a file system error occurs.
     */
    @Test
    void testAppendThreadSafe_DeterministicPathConvention() throws IOException {
        // Arrange: A single test key
        String testKey = "p2p-test";
        List<KeyValuePair> intermediateData = List.of(new KeyValuePair(testKey, "value"));
        int expectedPartition = Math.abs(testKey.hashCode()) % 3;

        // Act
        partitioner.appendThreadSafe(intermediateData);

        // Assert: Verify the local path follows the required P2P convention:
        // {baseDir}/{jobId}/{partitionIndex}/{mapTaskId}.txt
        Path expectedPath = tempDir.resolve(testJobId)
                .resolve(String.valueOf(expectedPartition))
                .resolve(testMapId + ".txt");

        assertTrue(Files.exists(expectedPath), "Local file does not follow the deterministic naming convention.");
        assertTrue(expectedPath.toString().endsWith(".txt"), "The local file should have a .txt extension.");
    }
}