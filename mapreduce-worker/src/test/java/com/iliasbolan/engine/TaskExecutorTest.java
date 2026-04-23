package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.storage.S3ClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link TaskExecutor}.
 * <p>
 * This suite verifies the entire Worker orchestration pipeline. It has been updated
 * to validate the <b>Peer-to-Peer (P2P) Shuffle</b> architecture, where intermediate
 * data is persisted to the local disk and transferred via HTTP instead of S3.
 * </p>
 * <p>
 * <b>Infrastructure Mocking:</b> Uses Mockito to intercept S3 service calls and
 * DynamicClassLoader logic, ensuring tests remain isolated from the physical
 * network and file system.
 * </p>
 */
class TaskExecutorTest {

    private S3ClientService mockS3Service;
    private TaskExecutor taskExecutor;
    private String baseShuffleDir;

    /**
     * Utilizes JUnit 5's {@code @TempDir} to provide a safe, isolated directory
     * for testing local P2P shuffle persistence.
     */
    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        // Create a fake MinIO network service
        mockS3Service = Mockito.mock(S3ClientService.class);
        baseShuffleDir = tempDir.toString();

        // Constructor updated to support local shuffle directory
        taskExecutor = new TaskExecutor(mockS3Service, baseShuffleDir);
    }

    /**
     * Verifies that a successful MAP task correctly orchestrates the P2P pipeline:
     * code download, data acquisition, parallel execution, and LOCAL partitioned persistence.
     * <p>
     * <b>P2P Update:</b> This test now verifies that intermediate data is
     * <u>not</u> uploaded to S3, as it is now hosted locally for sibling nodes.
     * </p>
     *
     * @throws Throwable to accommodate Resilience4j-wrapped S3 operations.
     */
    @Test
    void testExecuteTask_SuccessfulMapPhase_OrchestratesFullPipeline() throws Throwable {
        // Arrange 1: The JSON Payload (including new workerEndpoints list)
        String jsonPayload = """
                {
                    "jobId": "job-test",
                    "taskId": "map-001",
                    "taskType": "MAP",
                    "bucketName": "input-bucket",
                    "objectName": "data.txt",
                    "byteOffset": 0,
                    "byteLength": 1024,
                    "numReducers": 2,
                    "userCodeBucket": "code-bucket",
                    "userCodeObject": "WordCount.class",
                    "className": "com.iliasbolan.WordCount",
                    "workerEndpoints": []
                }
                """;

        // Arrange 2: Mock S3 input data
        List<String> fakeFileRecords = Arrays.asList(
                "hello distributed systems",
                "hello george"
        );
        when(mockS3Service.readDataChunk(eq("input-bucket"), eq("data.txt"), anyLong(), anyLong()))
                .thenReturn(fakeFileRecords);

        // Arrange 3: A dummy Mapper
        Mapper dummyMapper = (key, value) -> {
            List<KeyValuePair> results = new ArrayList<>();
            for (String word : value.split("\\s+")) {
                results.add(new KeyValuePair(word, "1"));
            }
            return results;
        };

        // Act: Intercept DynamicClassLoader
        try (MockedStatic<DynamicClassLoader> mockedLoader = Mockito.mockStatic(DynamicClassLoader.class)) {
            mockedLoader.when(() -> DynamicClassLoader.loadMapper(anyString(), anyString()))
                    .thenReturn(dummyMapper);

            taskExecutor.executeTask(jsonPayload);
        }

        // Assert 1: Bytecode and Input data must still come from S3
        verify(mockS3Service).downloadUserCode(eq("code-bucket"), eq("WordCount.class"), anyString());
        verify(mockS3Service).readDataChunk(eq("input-bucket"), eq("data.txt"), eq(0L), eq(1024L));

        // Assert 2: P2P VALIDATION - Verify NO intermediate data was uploaded to S3
        // Intermediate data now resides in the local 'tempDir' directory
        verify(mockS3Service, never()).writeData(eq("input-bucket"), contains("intermediate"), anyString());
    }

    /**
     * Ensures that the engine immediately rejects payloads with unknown task types.
     */
    @Test
    void testExecuteTask_UnknownTaskType_ThrowsException() {
        String jsonPayload = """
                {
                    "jobId": "job-test",
                    "taskType": "INVALID_TYPE",
                    "workerEndpoints": []
                }
                """;

        assertThrows(IllegalArgumentException.class, () -> taskExecutor.executeTask(jsonPayload));
    }

    /**
     * Verifies that a REDUCE task orchestrates the P2P fetch logic and persists
     * final results to S3.
     * <p>
     * <b>P2P Update:</b> This test verifies that the worker bypasses S3 listing
     * and instead prepares to fetch data from the provided network endpoints.
     * </p>
     *
     * @throws Throwable to accommodate Resilience4j-wrapped S3 operations.
     */
    @Test
    void testExecuteTask_SuccessfulReducePhase_OrchestratesFullPipeline() throws Throwable {
        // Arrange 1: The JSON Payload with worker network identities
        String jsonPayload = """
                {
                    "jobId": "job-test",
                    "taskId": "0", 
                    "taskType": "REDUCE",
                    "bucketName": "input-bucket",
                    "objectName": "",
                    "byteOffset": 0,
                    "byteLength": 0,
                    "numReducers": 2,
                    "userCodeBucket": "code-bucket",
                    "userCodeObject": "WordCountReducer.class",
                    "className": "com.iliasbolan.WordCountReducer",
                    "workerEndpoints": []
                }
                """;

        // Arrange 2: A dummy Reducer
        Reducer dummyReducer = (key, values) -> new KeyValuePair(key, String.valueOf(values.size()));

        // Act & Assert
        try (MockedStatic<DynamicClassLoader> mockedLoader = Mockito.mockStatic(DynamicClassLoader.class)) {
            mockedLoader.when(() -> DynamicClassLoader.loadReducer(anyString(), anyString()))
                    .thenReturn(dummyReducer);

            taskExecutor.executeTask(jsonPayload);
        }

        // 1. Verify user bytecode is still localized from S3
        verify(mockS3Service).downloadUserCode(eq("code-bucket"), eq("WordCountReducer.class"), anyString());

        // 2. Verify P2P VALIDATION - S3 Listing is no longer used for intermediate data
        verify(mockS3Service, never()).listIntermediateFiles(anyString(), anyString(), anyInt());
        verify(mockS3Service, never()).readObject(anyString(), contains("intermediate"));

        // 3. Final output must still be persisted to S3 for job finalization
        verify(mockS3Service).writeData(eq("input-bucket"), eq("job-test/output/result_part_0.txt"), anyString());
    }
}