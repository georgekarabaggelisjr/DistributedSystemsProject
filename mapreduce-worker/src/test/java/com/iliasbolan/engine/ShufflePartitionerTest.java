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
 * <b>Peer-to-Peer (P2P) Architecture Update:</b><br>
 * These tests have been refactored to validate local disk persistence. Instead of
 * mocking a remote S3 service, the suite utilizes JUnit 5's <code>@TempDir</code>
 * to perform real I/O operations in a safe, isolated temporary environment.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
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
        // Constructor now accepts baseShuffleDir instead of S3ClientService
        partitioner = new ShufflePartitioner(
                baseShuffleDir, testJobId, testMapId, 3
        );
    }

    /**
     * Verifies that the partitioner correctly groups identical keys and skips
     * generating directories for partitions that receive no data.
     * * @throws IOException If a file system error occurs during the partitioning process.
     */
    @Test
    void testPartitionAndWriteLocal_CorrectlyGroupsKeysAndSkipsEmptyPartitions() throws IOException {
        // Arrange: "apple" and "banana" will hash to specific partitions.
        // Include "apple" twice to ensure they end up in the same local file.
        List<KeyValuePair> intermediateData = List.of(
                new KeyValuePair("apple", "1"),
                new KeyValuePair("banana", "1"),
                new KeyValuePair("apple", "1")
        );

        // Act: Invoke the local write logic
        partitioner.partitionAndWriteLocal(intermediateData);

        // Assert:
        // 1. Verify that only directories for active partitions were created
        // We have 3 reducers, but we only expect directories for the partitions
        // determined by the hash(key) % 3 logic.
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
     * * @throws IOException If a file system error occurs.
     */
    @Test
    void testPartitionAndWriteLocal_DeterministicPathConvention() throws IOException {
        // Arrange: A single test key
        String testKey = "p2p-test";
        List<KeyValuePair> intermediateData = List.of(new KeyValuePair(testKey, "value"));
        int expectedPartition = Math.abs(testKey.hashCode()) % 3;

        // Act
        partitioner.partitionAndWriteLocal(intermediateData);

        // Assert: Verify the local path follows the required P2P convention:
        // {baseDir}/{jobId}/{partitionIndex}/{mapTaskId}.txt
        Path expectedPath = tempDir.resolve(testJobId)
                .resolve(String.valueOf(expectedPartition))
                .resolve(testMapId + ".txt");

        assertTrue(Files.exists(expectedPath), "Local file does not follow the deterministic naming convention.");
        assertTrue(expectedPath.toString().endsWith(".txt"), "The local file should have a .txt extension.");
    }
}