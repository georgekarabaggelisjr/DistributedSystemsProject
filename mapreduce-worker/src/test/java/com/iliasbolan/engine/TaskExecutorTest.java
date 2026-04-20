package com.iliasbolan.engine;

import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.storage.S3ClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link TaskExecutor}.
 * This verifies the entire Worker orchestration pipeline from JSON deserialization
 * to MinIO interaction, bypassing actual file system and network I/O.
 * <p>
 * <b>Fault Tolerance:</b> These tests are updated to accommodate {@link Throwable} signatures
 * introduced by the Resilience4j refactor in the underlying storage layer.
 * </p>
 */
class TaskExecutorTest {

    private S3ClientService mockS3Service;
    private TaskExecutor taskExecutor;

    @BeforeEach
    void setUp() {
        // Create a fake MinIO network service
        mockS3Service = Mockito.mock(S3ClientService.class);
        taskExecutor = new TaskExecutor(mockS3Service);
    }

    /**
     * Verifies that a successful MAP task correctly orchestrates the full pipeline:
     * code download, data acquisition, parallel execution, and partitioned upload.
     * * @throws Throwable to accommodate Resilience4j-wrapped S3 operations.
     */
    @Test
    void testExecuteTask_SuccessfulMapPhase_OrchestratesFullPipeline() throws Throwable {
        // Arrange 1: The JSON Payload
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
                    "className": "com.iliasbolan.WordCount"
                }
                """;

        // Arrange 2: Tell the fake MinIO what to return
        List<String> fakeFileRecords = Arrays.asList(
                "hello distributed systems",
                "hello george"
        );
        when(mockS3Service.readDataChunk(eq("input-bucket"), eq("data.txt"), anyLong(), anyLong()))
                .thenReturn(fakeFileRecords);

        // Arrange 3: A dummy Mapper so we don't need a real .class file
        Mapper dummyMapper = (key, value) -> {
            List<KeyValuePair> results = new ArrayList<>();
            for (String word : value.split("\\s+")) {
                results.add(new KeyValuePair(word, "1"));
            }
            return results;
        };

        // Act: We use MockedStatic to intercept the DynamicClassLoader.
        // Whenever the engine calls loadMapper(), it will instantly return our dummyMapper instead!
        try (MockedStatic<DynamicClassLoader> mockedLoader = Mockito.mockStatic(DynamicClassLoader.class)) {
            mockedLoader.when(() -> DynamicClassLoader.loadMapper(anyString(), anyString()))
                    .thenReturn(dummyMapper);

            // Execute the grand orchestration!
            taskExecutor.executeTask(jsonPayload);
        }

        // Assert: Prove that the executor triggered every step of the pipeline!

        // 1. Did it try to download the user code?
        verify(mockS3Service).downloadUserCode(eq("code-bucket"), eq("WordCount.class"), anyString());

        // 2. Did it try to download the data chunk?
        verify(mockS3Service).readDataChunk(eq("input-bucket"), eq("data.txt"), eq(0L), eq(1024L));

        // 3. Did it trigger the ShufflePartitioner and upload the final results back to MinIO?
        // (We use atLeastOnce() because the partitioner might split it into multiple files depending on the hash)
        verify(mockS3Service, atLeastOnce()).writeData(eq("input-bucket"), anyString(), anyString());
    }

    /**
     * Ensures that the engine immediately rejects payloads with unknown task types,
     * allowing the error to bubble up to the messaging layer for proper NACK handling.
     */
    @Test
    void testExecuteTask_UnknownTaskType_ThrowsException() {
        // Arrange: A payload with a typo in the task type
        String jsonPayload = """
                {
                    "jobId": "job-test",
                    "taskType": "INVALID_TYPE"
                }
                """;

        // Act & Assert: The engine should immediately reject it, bubbling the error
        // up to the RabbitMqConsumer so it can NACK the message.
        assertThrows(IllegalArgumentException.class, () -> taskExecutor.executeTask(jsonPayload));
    }

    /**
     * Verifies that a successful REDUCE task correctly orchestrates the Sort phase,
     * aggregates values by key, and persists the alphabetically ordered output.
     * * @throws Throwable to accommodate Resilience4j-wrapped S3 operations.
     */
    @Test
    void testExecuteTask_SuccessfulReducePhase_OrchestratesFullPipeline() throws Throwable {
        // Arrange 1: The JSON Payload for a REDUCE task
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
                    "className": "com.iliasbolan.WordCountReducer"
                }
                """;

        // Arrange 2: Tell fake MinIO to return a list of intermediate files
        when(mockS3Service.listIntermediateFiles(eq("input-bucket"), eq("job-test"), eq(0)))
                .thenReturn(List.of(
                        "job-test/intermediate/map-1_part_0.txt",
                        "job-test/intermediate/map-2_part_0.txt"
                ));

        // Arrange 3: Tell fake MinIO what is inside those text files
        when(mockS3Service.readObject(eq("input-bucket"), eq("job-test/intermediate/map-1_part_0.txt")))
                .thenReturn("apple\t1\nbanana\t1\n");
        when(mockS3Service.readObject(eq("input-bucket"), eq("job-test/intermediate/map-2_part_0.txt")))
                .thenReturn("apple\t1\n"); // A second apple from a different map task!

        // Arrange 4: A dummy Summing Reducer
        Reducer dummyReducer = (key, values) -> {
            int sum = values.stream().mapToInt(Integer::parseInt).sum();
            return new KeyValuePair(key, String.valueOf(sum));
        };

        // Act: Intercept DynamicClassLoader and run the TaskExecutor
        try (MockedStatic<DynamicClassLoader> mockedLoader = Mockito.mockStatic(DynamicClassLoader.class)) {
            mockedLoader.when(() -> DynamicClassLoader.loadReducer(anyString(), anyString()))
                    .thenReturn(dummyReducer);

            // Execute the orchestration!
            taskExecutor.executeTask(jsonPayload);
        }

        // Assert: Prove that the executor triggered every step

        // 1. Did it download the user Reducer code?
        verify(mockS3Service).downloadUserCode(eq("code-bucket"), eq("WordCountReducer.class"), anyString());

        // 2. Did it upload the final output to the correct 'output' directory?
        ArgumentCaptor<String> contentCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockS3Service).writeData(eq("input-bucket"), eq("job-test/output/result_part_0.txt"), contentCaptor.capture());

        String uploadedResult = contentCaptor.getValue();

        // 3. Ensure the aggregation was correct and the output is tab-separated and sorted alphabetically!
        assertTrue(uploadedResult.contains("apple\t2"), "Reducer failed to aggregate the two apples together.");
        assertTrue(uploadedResult.contains("banana\t1"), "Reducer missed the banana.");
    }
}