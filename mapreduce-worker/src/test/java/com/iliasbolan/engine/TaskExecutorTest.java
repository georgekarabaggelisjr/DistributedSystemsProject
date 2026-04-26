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
 * Unit test suite for the {@link TaskExecutor} orchestration engine.
 * <p>
 * This suite provides comprehensive validation for the Worker node's primary
 * computational controller. It utilizes advanced Mockito techniques, including
 * static mocking and construction mocking, to simulate a cloud-native
 * environment.
 * </p>
 * <p>
 * <b>Core Test Coverage:</b>
 * <ul>
 * <li><b>Resource Localization:</b> Verifying S3-to-Local handoffs for user code.</li>
 * <li><b>P2P Shuffle Logic:</b> Testing gRPC data plane integrity and token injection.</li>
 * <li><b>Fault Tolerance:</b> Validating <b>Reactive Lineage Recomputation</b> triggers.</li>
 * <li><b>Process Isolation:</b> Ensuring the Sandbox JVM lifecycle is correctly managed.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-25
 * @see TaskExecutor
 */
class TaskExecutorTest {

    private S3ClientService s3ClientService;
    private RabbitMqProducer eventProducer;
    private TaskExecutor taskExecutor;
    private final String nodeIp = "192.168.1.50";

    /**
     * JUnit 5 temporary directory used to simulate the local shuffle storage path.
     */
    @TempDir
    Path tempDir;

    /**
     * Initializes the testing context by mocking infrastructure dependencies
     * and injecting them into the TaskExecutor. This enforces strict
     * Inversion of Control (IoC) during testing.
     */
    @BeforeEach
    void setUp() {
        s3ClientService = mock(S3ClientService.class);
        eventProducer = mock(RabbitMqProducer.class);
        taskExecutor = new TaskExecutor(s3ClientService, tempDir.toString(), eventProducer, nodeIp);
    }

    /**
     * Validates a successful Map phase lifecycle.
     * <p>
     * This test ensures that the executor correctly localizes the user's Mapper code
     * and signals the Manager with the specific <code>HOSTNAME@@IP</code> format
     * required for <b>Data Locality Scheduling</b>.
     * </p>
     *
     * @throws Throwable If the internal orchestration logic or mocks fail unexpectedly.
     */
    @Test
    void testExecuteMapPhase_Success_SignalsCompletion() throws Throwable {
        // Arrange: Simulate a standard task intent from the Manager
        String jsonPayload = """
                {
                    "jobId": "job-123",
                    "taskId": "map-0",
                    "jobToken": "test-secure-token",
                    "taskType": "MAP",
                    "className": "com.example.WordCountMapper",
                    "userCodeBucket": "code",
                    "userCodeObject": "mapper.class"
                }
                """;

        Process mockProcess = mock(Process.class);
        when(mockProcess.waitFor()).thenReturn(0); // Simulate successful Sandbox exit

        // Mocking the ProcessBuilder construction to intercept Sandbox JVM spawning
        try (MockedConstruction<ProcessBuilder> mockedPb = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.start()).thenReturn(mockProcess);
                    when(mock.inheritIO()).thenReturn(mock);
                })) {

            taskExecutor.executeTask(jsonPayload);

            // Verify the Worker attempted to fetch the bytecode from S3
            verify(s3ClientService).downloadUserCode(eq("code"), eq("mapper.class"), anyString());

            // --- DATA LOCALITY VERIFICATION ---
            // In a headless test environment, NODE_NAME defaults to "unknown-node"
            String expectedEssEndpoint = "unknown-node@@" + nodeIp + ":7337";

            // Verify the completion signal includes the security token and the locality hint
            verify(eventProducer).sendCompletionSignal(
                    eq("job-123"),
                    eq("map-0"),
                    eq("test-secure-token"),
                    eq("COMPLETED"),
                    eq(expectedEssEndpoint)
            );
        }
    }

    /**
     * Validates the <b>Reactive Lineage Recomputation</b> sentinel logic.
     * <p>
     * This test simulates a network failure during a gRPC shuffle fetch. It
     * verifies that the executor intercepts the <code>StatusRuntimeException</code>
     * and transmits a targeted error signal to the Manager to trigger recovery
     * of the missing Map partition.
     * </p>
     *
     * @throws Throwable If the orchestration logic fails to handle the exception.
     */
    @Test
    void testExecuteReducePhase_ShuffleFetchFailed_SendsErrorSignal() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-999",
                    "taskId": "0",
                    "jobToken": "error-auth-token",
                    "taskType": "REDUCE",
                    "className": "com.test.Reducer",
                    "workerEndpoints": ["10.244.2.15:7337"]
                }
                """;

        ManagedChannel mockChannel = mock(ManagedChannel.class);
        ManagedChannelBuilder mockBuilder = mock(ManagedChannelBuilder.class);

        // Simulate a physical ESS node being offline
        StatusRuntimeException networkError = new StatusRuntimeException(Status.UNAVAILABLE.withDescription("ESS Node Offline"));

        // Utilize MockedStatic to intercept third-party gRPC builder calls
        try (MockedStatic<ManagedChannelBuilder> staticBuilder = Mockito.mockStatic(ManagedChannelBuilder.class);
             MockedStatic<ShuffleServiceGrpc> staticGrpc = Mockito.mockStatic(ShuffleServiceGrpc.class)) {

            staticBuilder.when(() -> ManagedChannelBuilder.forAddress(anyString(), anyInt())).thenReturn(mockBuilder);
            when(mockBuilder.usePlaintext()).thenReturn(mockBuilder);
            when(mockBuilder.keepAliveTime(anyLong(), any())).thenReturn(mockBuilder);
            when(mockBuilder.build()).thenReturn(mockChannel);
            when(mockChannel.shutdown()).thenReturn(mockChannel);

            // Force a failure during the stub initialization
            staticGrpc.when(() -> ShuffleServiceGrpc.newBlockingStub(any(ManagedChannel.class)))
                    .thenThrow(networkError);

            taskExecutor.executeTask(jsonPayload);

            // Assert: The Sentinel must report the failure signal to trigger orchestrator recovery
            verify(eventProducer).sendErrorSignal(
                    eq("job-999"),
                    eq("0"),
                    eq("error-auth-token"),
                    eq("FAILED"),
                    eq("SHUFFLE_FETCH_FAILED:map-chunk-0")
            );
        }
    }

    /**
     * Enforces the <b>Fail-Fast</b> protocol for isolated Sandbox crashes.
     * <p>
     * Verifies that if the child JVM (Sandbox) returns a non-zero exit code,
     * the parent process detects the failure and propagates it as a
     * RuntimeException.
     * </p>
     *
     * @throws Throwable If the process mocking encounters an issue.
     */
    @Test
    void testExecuteTask_SandboxCrash_ThrowsRuntimeException() throws Throwable {
        String jsonPayload = """
                {
                    "jobId": "job-fail",
                    "taskId": "map-002",
                    "jobToken": "fail-token",
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

            // Assert that the parent executor correctly identifies the child crash
            RuntimeException exception = assertThrows(RuntimeException.class, () -> taskExecutor.executeTask(jsonPayload));
            assertTrue(exception.getMessage().contains("exit code: 1"));
        }
    }

    /**
     * Validates input integrity by ensuring illegal task types are rejected.
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