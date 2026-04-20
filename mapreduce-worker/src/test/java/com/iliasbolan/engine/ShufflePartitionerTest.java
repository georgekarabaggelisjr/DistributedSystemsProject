package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.storage.S3ClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the {@link ShufflePartitioner}.
 * This verifies that the intermediate data is correctly hashed, partitioned,
 * and serialized before being uploaded to the shared file system.
 */
class ShufflePartitionerTest {

    private S3ClientService mockS3Service;
    private ShufflePartitioner partitioner;
    private final String testBucket = "test-bucket";
    private final String testJobId = "job-123";
    private final String testMapId = "map-001";

    @BeforeEach
    void setUp() {
        // Create a mock S3 service to avoid real network I/O
        mockS3Service = Mockito.mock(S3ClientService.class);

        // Initialize the partitioner with 3 target Reducers
        partitioner = new ShufflePartitioner(
                mockS3Service, testBucket, testJobId, testMapId, 3
        );
    }

    /**
     * Verifies that the partitioner correctly groups identical keys and skips
     * generating files for partitions that receive no data.
     * * @throws Throwable to accommodate Resilience4j-powered S3 signatures.
     */
    @Test
    void testPartitionAndUpload_CorrectlyGroupsKeysAndSkipsEmptyPartitions() throws Throwable {
        // Arrange: "apple" and "banana" will hash to specific partitions.
        // Include "apple" twice to ensure they end up in the same file.
        List<KeyValuePair> intermediateData = List.of(
                new KeyValuePair("apple", "1"),
                new KeyValuePair("banana", "1"),
                new KeyValuePair("apple", "1")
        );

        // Act
        partitioner.partitionAndUpload(intermediateData);

        // Assert:
        // 1. We have 3 reducers but only 2 unique keys.
        // Verification: writeData should be called exactly 2 times (skipping the empty partition).
        verify(mockS3Service, times(2)).writeData(eq(testBucket), anyString(), anyString());

        // 2. Use ArgumentCaptor to inspect the serialized content sent to S3
        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockS3Service, times(2)).writeData(eq(testBucket), anyString(), contentCaptor.capture());

        List<String> allUploadedContents = contentCaptor.getAllValues();

        // One of the uploads must contain both 'apple' entries separated by a tab and newline
        boolean foundGroupedApples = allUploadedContents.stream()
                .anyMatch(content -> content.contains("apple\t1\napple\t1\n"));

        assertTrue(foundGroupedApples, "Identical keys were not grouped into the same partition file.");
    }

    /**
     * Verifies that the partitioner adheres to the deterministic S3 naming
     * convention required for the subsequent Shuffle/Sort phase.
     * * @throws Throwable to accommodate Resilience4j-powered S3 signatures.
     */
    @Test
    void testPartitionAndUpload_DeterministicNamingConvention() throws Throwable {
        // Arrange
        List<KeyValuePair> intermediateData = List.of(new KeyValuePair("test", "1"));

        // Act
        partitioner.partitionAndUpload(intermediateData);

        // Assert: Verify the S3 path follows the standard Map-Reduce naming convention:
        // [jobId]/intermediate/[taskId]_part_[partitionIndex].txt
        ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockS3Service).writeData(eq(testBucket), pathCaptor.capture(), anyString());

        String capturedPath = pathCaptor.getValue();
        assertTrue(capturedPath.startsWith(testJobId + "/intermediate/" + testMapId + "_part_"),
                "The S3 object path does not follow the required naming convention.");
        assertTrue(capturedPath.endsWith(".txt"), "The intermediate file should have a .txt extension.");
    }
}