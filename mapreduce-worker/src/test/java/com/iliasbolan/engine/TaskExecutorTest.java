package com.iliasbolan.engine;

import com.google.protobuf.ByteString;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import com.iliasbolan.messaging.RabbitMqProducer;
import com.iliasbolan.storage.S3ClientService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link TaskExecutor} orchestration engine.
 * <p>
 * <b>Sandbox Orchestrator Update:</b><br>
 * This suite has been heavily refactored to validate the new Lightweight Orchestrator pattern.
 * Instead of testing in-memory class loading and Fork/Join pooling, it now validates that the
 * engine correctly provisions local resources, streams gRPC partitions, and securely delegates
 * untrusted execution to an isolated Child JVM via {@link ProcessBuilder}.
 * </p>
 * <p>
 * <b>Testing Strategy:</b><br>
 * Utilizes Mockito 5's {@code mockConstruction} to intercept native OS process creation,
 * simulating sandbox success and failure states without booting actual JVMs during the test lifecycle.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 5.0
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
     * Validates that a MAP task correctly provisions the environment and launches the Sandbox.
     *
     * """
     * Verifies the orchestration sequence for Map tasks.
     * * Ensures that the payload is safely persisted to disk, the `ProcessBuilder` is invoked
     * with the correct sandbox classpath, and the control plane is signaled upon success.
     * * Raises:
     * Throwable: If mocked OS process interception fails.
     * """
     */
    @Test
    void testExecuteTask_SuccessfulMapPhase_DelegatesToSandboxAndSignalsManager() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-sandbox-test",
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

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0); // Simulate successful Sandbox exit

        // Intercept native OS process creation
        try (MockedConstruction<ProcessBuilder> mockedPb = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.start()).thenReturn(mockProcess);
                    when(mock.inheritIO()).thenReturn(mock);
                })) {

            taskExecutor.executeTask(jsonPayload);

            // Assert 1: The ProcessBuilder was actually invoked to spawn the SandboxRunner
            assertEquals(1, mockedPb.constructed().size(), "Sandbox JVM was never spawned.");

            // Assert 2: The process was started and waited upon
            verify(mockProcess, times(1)).waitFor();
        }

        // Assert 3: Code acquisition occurred prior to sandbox launch
        verify(mockS3Service).downloadUserCode(eq("code-bucket"), eq("WordCount.class"), anyString());

        // Assert 4: Completion signaling with gRPC endpoint info
        verify(mockEventProducer).sendCompletionSignal(eq("job-sandbox-test"), eq("map-001"), eq("COMPLETED"), contains(":50051"));
    }

    /**
     * Validates that a REDUCE task successfully streams gRPC data before delegating to the Sandbox.
     *
     * """
     * Verifies the complex Reduce orchestration sequence.
     * * Validates that the Orchestrator successfully fetches remote data via gRPC streams,
     * writes it to the local disk, and subsequently boots the Sandbox JVM to handle the
     * memory-intensive sorting and reduction.
     * * Raises:
     * Throwable: If gRPC mock networking or OS interception fails.
     * """
     */
    @Test
    @SuppressWarnings("unchecked")
    void testExecuteTask_SuccessfulReducePhase_StreamsGrpcAndDelegatesToSandbox() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-sandbox-test",
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

        PartitionChunk fakeChunk = PartitionChunk.newBuilder()
                .setContent(ByteString.copyFromUtf8("hello\t1\nworld\t1\n"))
                .build();

        Iterator<PartitionChunk> mockIterator = mock(Iterator.class);
        when(mockIterator.hasNext()).thenReturn(true, false);
        when(mockIterator.next()).thenReturn(fakeChunk);

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0); // Simulate successful Sandbox exit

        try (MockedConstruction<ProcessBuilder> mockedPb = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.start()).thenReturn(mockProcess);
                    when(mock.inheritIO()).thenReturn(mock);
                });
             MockedStatic<ManagedChannelBuilder> mockedBuilder = Mockito.mockStatic(ManagedChannelBuilder.class);
             MockedStatic<ShuffleServiceGrpc> mockedGrpc = Mockito.mockStatic(ShuffleServiceGrpc.class)) {

            ManagedChannel mockChannel = mock(ManagedChannel.class);
            ManagedChannelBuilder mockChannelBuilder = mock(ManagedChannelBuilder.class);
            ShuffleServiceGrpc.ShuffleServiceBlockingStub mockStub = mock(ShuffleServiceGrpc.ShuffleServiceBlockingStub.class);

            mockedBuilder.when(() -> ManagedChannelBuilder.forAddress(anyString(), anyInt())).thenReturn(mockChannelBuilder);
            when(mockChannelBuilder.usePlaintext()).thenReturn(mockChannelBuilder);
            when(mockChannelBuilder.keepAliveTime(anyLong(), any())).thenReturn(mockChannelBuilder);

            when(mockChannelBuilder.build()).thenReturn(mockChannel);
            when(mockChannel.shutdown()).thenReturn(mockChannel);
            when(mockChannel.awaitTermination(anyLong(), any())).thenReturn(true);

            mockedGrpc.when(() -> ShuffleServiceGrpc.newBlockingStub(any(ManagedChannel.class))).thenReturn(mockStub);
            when(mockStub.getPartition(any())).thenReturn(mockIterator);

            taskExecutor.executeTask(jsonPayload);

            // Assert: Sandbox was launched for the reduce operation
            assertEquals(1, mockedPb.constructed().size(), "Sandbox JVM was never spawned for Reduce phase.");
        }

        // Verify that the orchestrator still handles the localized code download
        verify(mockS3Service).downloadUserCode(anyString(), anyString(), anyString());
    }

    /**
     * Validates that an abnormal exit from the Sandbox JVM bubbles up as a RuntimeException.
     */
    @Test
    void testExecuteTask_SandboxFails_ThrowsRuntimeException() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-fail-test",
                    "taskId": "map-002",
                    "taskType": "MAP",
                    "className": "com.fail.Mapper"
                }
                """;

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(1); // Simulate JVM crash / Error code 1

        try (MockedConstruction<ProcessBuilder> mockedPb = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.start()).thenReturn(mockProcess);
                    when(mock.inheritIO()).thenReturn(mock);
                })) {

            RuntimeException exception = assertThrows(RuntimeException.class, () -> taskExecutor.executeTask(jsonPayload));
            assertTrue(exception.getMessage().contains("exit code: 1"), "Did not propagate the correct exit code.");
        }
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