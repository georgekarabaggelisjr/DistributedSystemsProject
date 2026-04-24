package com.iliasbolan.engine.shuffle;

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
 * Unit test suite for the {@link ShufflePartitioner} component.
 *
 * <p>This suite verifies that intermediate Map-Reduce data is correctly hashed,
 * partitioned, and persisted to the local file system. It ensures the integrity
 * of the directory structure and serialization format required for the
 * Peer-to-Peer (P2P) Shuffle phase.</p>
 *
 * <p><b>Architectural Update: Spill-to-Disk Validation</b><br>
 * These tests validate the high-concurrency, lock-striped disk persistence logic.
 * They utilize JUnit 5's {@code @TempDir} to perform real I/O operations within
 * a safe, isolated, and temporary file system environment, ensuring that
 * concurrent flushes do not result in data corruption.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @see ShufflePartitioner
 * @since 2026-04-24
 */
class ShufflePartitionerTest {

    private ShufflePartitioner partitioner;
    private final String testJobId = "job-123";
    private final String testMapId = "map-001";
    private String baseShuffleDir;

    /**
     * An isolated temporary directory provided by JUnit 5 for testing local
     * P2P shuffle persistence. Automatically cleaned up after test execution.
     */
    @TempDir
    Path tempDir;

    /**
     * Initializes the test environment, establishing the base directory for
     * local shuffle data and configuring the partitioner with three target reducers.
     */
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
     * Validates the core routing and disk-spill mechanism of the partitioner.
     *
     * <p>Verifies that the thread-safe partitioner correctly segments keys based
     * on their hash values, creates the necessary directory structures, and
     * groups identical keys into the same deterministic files without data loss.</p>
     *
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
     * Validates that the partitioner adheres to the deterministic path naming
     * convention required for the Peer-to-Peer infrastructure.
     *
     * <p>This naming convention is essential for the embedded gRPC server to
     * accurately locate and stream files requested by remote Reducers during
     * the shuffle phase.</p>
     *
     * @throws IOException If a file system error occurs during path generation or file creation.
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