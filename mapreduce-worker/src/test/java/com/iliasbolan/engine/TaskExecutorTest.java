package com.iliasbolan.engine;

import com.google.protobuf.ByteString;
import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import com.iliasbolan.messaging.RabbitMqProducer;
import com.iliasbolan.storage.S3ClientService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link TaskExecutor} orchestration engine.
 * <p>
 * <b>Spill-to-Disk Architecture Update:</b><br>
 * This suite has been upgraded to validate the end-to-end memory-safe pipeline.
 * It verifies that the `MapTaskProcessor` correctly bypasses list aggregation and
 * flushes directly to the local disk, and proves that the `ExternalMergeSorter`
 * successfully reads raw gRPC streams, sorts them in memory bounds, and delegates
 * them to the Batch Processor.
 * </p>
 * <p>
 * <b>Testing Strategy:</b><br>
 * Utilizes Mockito for dependency injection and {@code MockedStatic} for intercepting
 * dynamic class loading and gRPC network construction. Real file I/O is permitted
 * within the JUnit 5 {@code @TempDir} to fully test the External Merge Sort mechanics.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 4.0
 * @since 2026-04-24
 */
class TaskExecutorTest {

    private S3ClientService mockS3Service;
    private RabbitMqProducer mockEventProducer;
    private TaskExecutor taskExecutor;
    private String baseShuffleDir;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        mockS3Service = Mockito.mock(S3ClientService.class);
        mockEventProducer = Mockito.mock(RabbitMqProducer.class);
        baseShuffleDir = tempDir.toString();

        // Pass a dummy Pod IP for local testing
        taskExecutor = new TaskExecutor(mockS3Service, baseShuffleDir, mockEventProducer, "127.0.0.1");
    }

    /**
     * Validates that a MAP task correctly executes the full memory-safe pipeline.
     *
     * """
     * Verifies the end-to-end Map phase with localized disk-spilling.
     * * Ensures that the dynamic Map logic processes the chunk and correctly
     * invokes the thread-safe `ShufflePartitioner` to write intermediate files
     * to the container's disk before signaling the Orchestrator.
     * * Raises:
     * Throwable: If any mocked I/O or reflection phase fails.
     * """
     * @throws Throwable if any stage of the map phase fails.
     */
    @Test
    void testExecuteTask_SuccessfulMapPhase_OrchestratesFullPipelineAndSpillsToDisk() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-grpc-test",
                    "taskId": "map-001",
                    "taskType": "MAP",
                    "bucketName": "input-bucket",
                    "objectName": "data.txt",
                    "byteOffset": 0,
                    "byteLength": 1024,
                    "numReducers": 1,
                    "userCodeBucket": "code-bucket",
                    "userCodeObject": "WordCount.class",
                    "className": "com.iliasbolan.WordCount",
                    "workerEndpoints": []
                }
                """;

        when(mockS3Service.readDataChunk(anyString(), anyString(), anyLong(), anyLong()))
                .thenReturn(Arrays.asList("distributed systems", "grpc shuffle"));

        Mapper dummyMapper = (key, value) -> Collections.singletonList(new KeyValuePair(value, "1"));

        try (MockedStatic<DynamicClassLoader> mockedLoader = Mockito.mockStatic(DynamicClassLoader.class)) {
            mockedLoader.when(() -> DynamicClassLoader.loadMapper(anyString(), anyString()))
                    .thenReturn(dummyMapper);

            taskExecutor.executeTask(jsonPayload);
        }

        // Verify code acquisition
        verify(mockS3Service).downloadUserCode(eq("code-bucket"), eq("WordCount.class"), anyString());

        // Assert 1: Verify intermediate data is NOT written to S3 in the P2P model
        verify(mockS3Service, never()).writeData(eq("input-bucket"), contains("intermediate"), anyString());

        // Assert 2: Verify the Spill-to-Disk actually wrote to the local partition directory
        // Since numReducers = 1, everything hashes to partition '0'
        Path expectedSpillFile = Paths.get(baseShuffleDir, "job-grpc-test", "0", "map-001.txt");
        assertTrue(Files.exists(expectedSpillFile), "The Map phase failed to spill intermediate results to the local disk.");

        // Assert 3: Verify completion signaling with gRPC endpoint info
        verify(mockEventProducer).sendCompletionSignal(anyString(), anyString(), eq("COMPLETED"), contains(":50051"));
    }

    /**
     * Validates that a REDUCE task successfully streams data via gRPC and applies the External Merge Sort.
     *
     * """
     * Verifies the complex Reduce phase utilizing Disk Spills and K-Way Merge processing.
     * * Simulates a high-performance gRPC stream, validating that the engine writes
     * the raw stream to disk, correctly sorts the file using the ExternalMergeSorter,
     * reduces the batches, and uploads the accurate final string to S3.
     * * Raises:
     * Throwable: If gRPC streaming, external sorting, or S3 persistence fails.
     * """
     * @throws Throwable if gRPC streaming or final reduction fails.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testExecuteTask_SuccessfulReducePhase_ExternalMergeSortsAndPersistsToS3() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-grpc-test",
                    "taskId": "0", 
                    "taskType": "REDUCE",
                    "bucketName": "input-bucket",
                    "numReducers": 1,
                    "userCodeBucket": "code-bucket",
                    "userCodeObject": "Reducer.class",
                    "className": "com.iliasbolan.WordCountReducer",
                    "workerEndpoints": ["10.0.0.1:50051"]
                }
                """;

        // Dummy reducer counts the number of occurrences of a key
        Reducer dummyReducer = (key, values) -> new KeyValuePair(key, String.valueOf(values.size()));

        // Create a raw mock stream simulating network delivery
        PartitionChunk fakeChunk = PartitionChunk.newBuilder()
                .setContent(ByteString.copyFromUtf8("hello\t1\nworld\t1\nhello\t1\n"))
                .build();

        Iterator<PartitionChunk> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next()).thenReturn(fakeChunk);

        try (MockedStatic<DynamicClassLoader> mockedLoader = Mockito.mockStatic(DynamicClassLoader.class);
             MockedStatic<ManagedChannelBuilder> mockedBuilder = Mockito.mockStatic(ManagedChannelBuilder.class);
             MockedStatic<ShuffleServiceGrpc> mockedGrpc = Mockito.mockStatic(ShuffleServiceGrpc.class)) {

            mockedLoader.when(() -> DynamicClassLoader.loadReducer(anyString(), anyString())).thenReturn(dummyReducer);

            ManagedChannel mockChannel = mock(ManagedChannel.class);
            ManagedChannelBuilder mockChannelBuilder = mock(ManagedChannelBuilder.class);
            ShuffleServiceGrpc.ShuffleServiceBlockingStub mockStub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);

            // Mock the builder chain
            mockedBuilder.when(() -> ManagedChannelBuilder.forAddress(anyString(), anyInt())).thenReturn(mockChannelBuilder);
            when(mockChannelBuilder.usePlaintext()).thenReturn(mockChannelBuilder);
            when(mockChannelBuilder.keepAliveTime(anyLong(), any())).thenReturn(mockChannelBuilder);

            when(mockChannelBuilder.build()).thenReturn(mockChannel);
            when(mockChannel.shutdown()).thenReturn(mockChannel);
            when(mockChannel.awaitTermination(anyLong(), any())).thenReturn(true);

            mockedGrpc.when(() -> ShuffleServiceGrpc.newBlockingStub(any(ManagedChannel.class))).thenReturn(mockStub);
            when(mockStub.getPartition(any())).thenReturn(mockIterator);

            taskExecutor.executeTask(jsonPayload);
        }

        // Assert 1: Legacy listing is completely bypassed
        verify(mockS3Service, never()).listIntermediateFiles(anyString(), anyString(), anyInt());

        // Assert 2: Capture the S3 upload to prove the External Merge Sorter successfully processed the disk files
        ArgumentCaptor<String> dataCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockS3Service).writeData(eq("input-bucket"), eq("job-grpc-test/output/result_part_0.txt"), dataCaptor.capture());

        // 'hello' appeared twice, 'world' appeared once.
        // The ExternalMergeSorter guarantees alphabetical total order output.
        String finalOutput = dataCaptor.getValue();
        assertEquals("hello\t2\nworld\t1\n", finalOutput, "External Merge Sorter failed to correctly group and reduce the disk-spilled batches.");
    }

    /**
     * Validates that an unsupported task type triggers the appropriate exception.
     */
    @Test
    void testExecuteTask_UnknownTaskType_ThrowsIllegalArgumentException() {
        String jsonPayload = """
                {
                    "jobId": "job-test",
                    "taskType": "CORRUPT_TYPE"
                }
                """;

        assertThrows(IllegalArgumentException.class, () -> taskExecutor.executeTask(jsonPayload));
    }
}