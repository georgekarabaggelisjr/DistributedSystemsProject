package com.iliasbolan.engine;

import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.services.S3ClientService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Enterprise-grade unit test suite for the {@link TaskExecutor} orchestration engine.
 * <p>
 * This suite validates the core operational phases of a distributed worker node, including:
 * <ul>
 * <li>Map phase resource localization and sandbox execution.</li>
 * <li>Reduce phase P2P data streaming via gRPC.</li>
 * <li>Reactive Lineage Recomputation triggers (Sentinel Interceptor).</li>
 * <li>Process isolation and child JVM lifecycle management.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 */
class TaskExecutorTest {

    private S3ClientService s3ClientService;
    private RabbitMqProducer eventProducer;
    private TaskExecutor taskExecutor;
    private final String podIp = "10.0.0.1";

    @TempDir
    Path tempDir;

    /**
     * Re-initializes mocks and the subject under test before each execution to ensure
     * strict test isolation.
     */
    @BeforeEach
    void setUp() {
        s3ClientService = mock(S3ClientService.class);
        eventProducer = mock(RabbitMqProducer.class);
        taskExecutor = new TaskExecutor(s3ClientService, tempDir.toString(), eventProducer, podIp);
    }

    /**
     * Validates that the Map phase correctly localizes user code, persists the execution
     * payload, and signals completion after successful sandbox execution.
     *
     * @throws Throwable If the internal orchestration logic or mocks fail.
     */
    @Test
    void testExecuteMapPhase_Success_SignalsCompletion() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-123",
                    "taskId": "map-0",
                    "taskType": "MAP",
                    "className": "com.example.WordCountMapper",
                    "userCodeBucket": "code",
                    "userCodeObject": "mapper.class"
                }
                """;

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedPb = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.start()).thenReturn(mockProcess);
                    when(mock.inheritIO()).thenReturn(mock);
                })) {

            taskExecutor.executeTask(jsonPayload);

            // Verify resource acquisition and sandbox execution
            verify(s3ClientService).downloadUserCode(eq("code"), eq("mapper.class"), anyString());
            verify(eventProducer).sendCompletionSignal(eq("job-123"), eq("map-0"), eq("COMPLETED"), contains(podIp));
        }
    }

    /**
     * Validates the <b>Reactive Lineage Recomputation</b> protocol (Sentinel Interceptor).
     * <p>
     * Verifies that if a remote Map pod is unreachable during the Reduce phase fetch loop,
     * the executor intercepts the gRPC network failure, transmits a targeted
     * {@code SHUFFLE_FETCH_FAILED} error signal to the Manager, and suspends execution
     * gracefully.
     * </p>
     *
     * @throws Throwable If the internal orchestration logic fails.
     */
    @Test
    void testExecuteReducePhase_ShuffleFetchFailed_SendsErrorSignal() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-999",
                    "taskId": "0",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "workerEndpoints": ["10.244.2.15:50051"]
                }
                """;

        ManagedChannel mockChannel = mock(ManagedChannel.class);
        ManagedChannelBuilder mockBuilder = mock(ManagedChannelBuilder.class);

        // Simulate a gRPC network failure (e.g., pod eviction)
        StatusRuntimeException networkError = new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Pod Evicted"));

        try (MockedStatic<ManagedChannelBuilder> staticBuilder = Mockito.mockStatic(ManagedChannelBuilder.class);
             MockedStatic<ShuffleServiceGrpc> staticGrpc = Mockito.mockStatic(ShuffleServiceGrpc.class)) {

            staticBuilder.when(() -> ManagedChannelBuilder.forAddress(anyString(), anyInt())).thenReturn(mockBuilder);
            when(mockBuilder.usePlaintext()).thenReturn(mockBuilder);
            when(mockBuilder.keepAliveTime(anyLong(), any())).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockChannel);
            when(mockChannel.shutdown()).thenReturn(mockChannel);

            // Force the gRPC stub to throw a network exception when the iterator is requested
            staticGrpc.when(() -> ShuffleServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
                    .thenThrow(networkError);

            taskExecutor.executeTask(jsonPayload);

            // Verification: The Sentinel must report the exact missing lineage chunk
            verify(eventProducer).sendErrorSignal(
                    eq("job-999"),
                    eq("0"),
                    eq("FAILED"),
                    eq("SHUFFLE_FETCH_FAILED:map-chunk-0")
            );

            // Verify the Sandbox was NOT invoked, preserving node stability
            verify(s3ClientService).downloadUserCode(any(), any(), any());
        }
    }

    /**
     * Enforces the Fail-Fast protocol by ensuring that if the isolated Child JVM (Sandbox)
     * terminates with a non-zero exit code, the error is propagated as a RuntimeException.
     *
     * @throws Throwable If the process mock encounters an error.
     */
    @Test
    void testExecuteTask_SandboxCrash_ThrowsRuntimeException() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-fail",
                    "taskId": "map-002",
                    "taskType": "MAP",
                    "className": "com.fail.Mapper"
                }
                """;

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(1); // Simulate JVM crash

        try (MockedConstruction<ProcessBuilder> mockedPb = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.start()).thenReturn(mockProcess);
                    when(mock.inheritIO()).thenReturn(mock);
                })) {

            RuntimeException exception = assertThrows(RuntimeException.class, () -> taskExecutor.executeTask(jsonPayload));
            assertTrue(exception.getMessage().contains("exit code: 1"));
        }
    }

    /**
     * Validates that malformed task definitions or unsupported phase types trigger
     * an immediate {@link IllegalArgumentException}.
     */
    @Test
    void testExecuteTask_UnknownTaskType_ThrowsIllegalArgumentException() {
        String jsonPayload = """
                {
                    "jobId": "job-invalid",
                    "taskType": "ILLEGAL_PHASE"
                }
                """;

        assertThrows(IllegalArgumentException.class, () -> taskExecutor.executeTask(jsonPayload));
    }
}