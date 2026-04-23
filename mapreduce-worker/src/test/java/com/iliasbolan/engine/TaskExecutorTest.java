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
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link TaskExecutor} orchestration engine.
 * <p>
 * This suite validates the <b>Hybrid gRPC Shuffle</b> architecture, ensuring that
 * the execution engine correctly coordinates between S3 for persistent storage,
 * RabbitMQ for lifecycle signaling, and gRPC for peer-to-peer data streaming.
 * </p>
 * <p>
 * <b>Testing Strategy:</b><br>
 * Utilizes Mockito for dependency injection and {@code MockedStatic} for intercepting
 * dynamic class loading and gRPC network construction. This allows for full pipeline
 * validation without requiring a live Kubernetes cluster or active gRPC server.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.2
 * @since 2026-04-23
 */
class TaskExecutorTest {

    /** Mocked service for S3-compatible storage interactions. */
    private S3ClientService mockS3Service;

    /** Mocked producer for signaling task events to the Manager. */
    private RabbitMqProducer mockEventProducer;

    /** The instance under test. */
    private TaskExecutor taskExecutor;

    /** Path to the temporary directory used for local shuffle data simulation. */
    private String baseShuffleDir;

    /** * JUnit 5 extension to provide a temporary directory for each test run,
     * ensuring filesystem isolation.
     */
    @TempDir
    Path tempDir;

    /**
     * Initializes the testing environment before each test execution.
     * <p>
     * <b>Correction:</b> Now includes the required {@code podIp} argument to match
     * the updated {@link TaskExecutor} constructor signature.
     * </p>
     */
    @BeforeEach
    void setUp() {
        mockS3Service = Mockito.mock(S3ClientService.class);
        mockEventProducer = Mockito.mock(RabbitMqProducer.class);
        baseShuffleDir = tempDir.toString();

        // Pass a dummy Pod IP for local testing
        taskExecutor = new TaskExecutor(mockS3Service, baseShuffleDir, mockEventProducer, "127.0.0.1");
    }

    /**
     * Validates that a MAP task correctly executes the full gRPC-ready pipeline.
     * <p>
     * This test ensures that the mapper downloads user code, processes data chunks,
     * partitions the results locally, and finally signals completion with its
     * gRPC network coordinates to the Manager.
     * </p>
     * * @throws Throwable if any stage of the map phase fails.
     */
    @Test
    void testExecuteTask_SuccessfulMapPhase_OrchestratesFullPipelineAndSignalsManager() throws Throwable {
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

        // Verify that intermediate data is NOT written to S3 in the gRPC P2P model
        verify(mockS3Service, never()).writeData(eq("input-bucket"), contains("intermediate"), anyString());

        // Verify completion signaling with gRPC endpoint info
        verify(mockEventProducer).sendCompletionSignal(anyString(), anyString(), eq("COMPLETED"), contains(":50051"));
    }

    /**
     * Validates that a REDUCE task successfully streams data via gRPC.
     * <p>
     * This test mocks the gRPC networking layer using {@code MockedStatic} for
     * {@link ManagedChannelBuilder} to simulate the high-performance streaming
     * of intermediate data from sibling nodes.
     * </p>
     * <p>
     * <b>Correction:</b> Explicitly stubs the {@code keepAliveTime} method on the
     * builder mock to support fluent method chaining without {@code NullPointerException}.
     * </p>
     * * @throws Throwable if gRPC streaming or final reduction fails.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testExecuteTask_SuccessfulReducePhase_StreamsViaGrpcAndPersistsToS3() throws Throwable {
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

        Reducer dummyReducer = (key, values) -> new KeyValuePair(key, String.valueOf(values.size()));

        PartitionChunk fakeChunk = PartitionChunk.newBuilder()
                .setContent(ByteString.copyFromUtf8("hello\t1\nworld\t1\n"))
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

            // FIX: Ensure fluent builder chaining returns the mock builder
            when(mockChannelBuilder.keepAliveTime(anyLong(), any())).thenReturn(mockChannelBuilder);

            when(mockChannelBuilder.build()).thenReturn(mockChannel);
            when(mockChannel.shutdown()).thenReturn(mockChannel);
            when(mockChannel.shutdownNow()).thenReturn(mockChannel);
            when(mockChannel.awaitTermination(anyLong(), any())).thenReturn(true);

            mockedGrpc.when(() -> ShuffleServiceGrpc.newBlockingStub(any(ManagedChannel.class))).thenReturn(mockStub);
            when(mockStub.getPartition(any())).thenReturn(mockIterator);

            taskExecutor.executeTask(jsonPayload);
        }

        // Verify that legacy listing is bypassed and final results are persisted to S3
        verify(mockS3Service, never()).listIntermediateFiles(anyString(), anyString(), anyInt());
        verify(mockS3Service).writeData(eq("input-bucket"), eq("job-grpc-test/output/result_part_0.txt"), anyString());
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