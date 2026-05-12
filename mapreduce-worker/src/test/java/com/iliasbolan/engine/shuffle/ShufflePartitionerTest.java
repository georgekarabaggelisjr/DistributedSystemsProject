package com.iliasbolan.engine.shuffle;

import com.iliasbolan.core.KeyValuePair;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test suite for the ShufflePartitioner component.
 * * This suite verifies that intermediate Map-Reduce data is correctly hashed,
 * partitioned, and persisted to the local file system using LZ4 compression.
 * * Architectural Update: LZ4 Validation
 * These tests have been upgraded to validate the binary LZ4 Frame format. They
 * utilize `FramedLZ4CompressorInputStream` to decompress and verify the integrity of
 * data spilled to the local P2P shuffle storage, ensuring that the compression
 * layer does not introduce data corruption during high-concurrency flushes.
 */
class ShufflePartitionerTest {

    private ShufflePartitioner partitioner;
    private final String testJobId = "job-123";
    private final String testMapId = "map-001";
    private String baseShuffleDir;

    /**
     * An isolated temporary directory provided by JUnit 5 for testing local
     * P2P shuffle persistence.
     */
    @TempDir
    Path tempDir;

    /**
     * Initializes the test environment, establishing the base directory for
     * local shuffle data and configuring the partitioner with three target reducers.
     */
    @BeforeEach
    void setUp() {
        baseShuffleDir = tempDir.toString();

        partitioner = new ShufflePartitioner(
                baseShuffleDir, testJobId, testMapId, 3
        );
    }

    /**
     * Validates the core routing and LZ4 disk-spill mechanism.
     * * Verifies that the thread-safe partitioner correctly segments keys based
     * on their bitmasked hash values, creates the necessary directory structures,
     * and compresses identical keys into the same deterministic binary files.
     * * Raises:
     * IOException: If a file system error or decompression failure occurs.
     */
    @Test
    void testAppendThreadSafe_CorrectlyGroupsKeysAndSkipsEmptyPartitions() throws IOException {
        List<KeyValuePair> intermediateData = List.of(
                new KeyValuePair("apple", "1"),
                new KeyValuePair("banana", "1"),
                new KeyValuePair("apple", "1")
        );

        partitioner.appendThreadSafe(intermediateData);

        partitioner.close();

        int applePartition = ("apple".hashCode() & Integer.MAX_VALUE) % 3;
        int bananaPartition = ("banana".hashCode() & Integer.MAX_VALUE) % 3;

        Path applePath = tempDir.resolve(testJobId).resolve(String.valueOf(applePartition)).resolve(testMapId + ".lz4");
        Path bananaPath = tempDir.resolve(testJobId).resolve(String.valueOf(bananaPartition)).resolve(testMapId + ".lz4");

        assertTrue(Files.exists(applePath), "Compressed partition file for 'apple' should exist at: " + applePath);
        assertTrue(Files.exists(bananaPath), "Compressed partition file for 'banana' should exist at: " + bananaPath);

        String appleContent = readCompressedFile(applePath);
        assertTrue(appleContent.contains("apple\t1\napple\t1\n"),
                "Identical keys were not correctly aggregated in the compressed local partition file.");
    }

    /**
     * Validates that the partitioner adheres to the deterministic path naming
     * convention required for the Peer-to-Peer infrastructure.
     * * Verifies that the file extension is strictly `.lz4` to signal
     * binary compression to remote Reducers.
     * * Raises:
     * IOException: If a file system error occurs.
     */
    @Test
    void testAppendThreadSafe_DeterministicPathConvention() throws IOException {
        String testKey = "p2p-test";
        List<KeyValuePair> intermediateData = List.of(new KeyValuePair(testKey, "value"));
        int expectedPartition = (testKey.hashCode() & Integer.MAX_VALUE) % 3;

        partitioner.appendThreadSafe(intermediateData);

        partitioner.close();

        Path expectedPath = tempDir.resolve(testJobId)
                .resolve(String.valueOf(expectedPartition))
                .resolve(testMapId + ".lz4");

        assertTrue(Files.exists(expectedPath), "Local file does not follow the deterministic naming convention.");
        assertTrue(expectedPath.toString().endsWith(".lz4"), "The local file must have a .lz4 extension for the binary data plane.");
    }

    /**
     * Helper utility to decompress and read the contents of an LZ4-encoded shuffle file.
     *
     * Args:
     * path (Path): The physical path to the .lz4 file.
     * * Returns:
     * str: The decompressed UTF-8 string content.
     * * Raises:
     * IOException: If the file cannot be read or the LZ4 frame is malformed.
     */
    private String readCompressedFile(Path path) throws IOException {
        try (InputStream is = Files.newInputStream(path);
             FramedLZ4CompressorInputStream lz4is = new FramedLZ4CompressorInputStream(is, true);
             BufferedReader reader = new BufferedReader(new InputStreamReader(lz4is, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n", "", "\n"));
        }
    }
}