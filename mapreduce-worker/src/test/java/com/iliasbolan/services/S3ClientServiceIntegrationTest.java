package com.iliasbolan.services;

import com.iliasbolan.infrastructure.MinioConnectionManager;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test suite for the {@link S3ClientService}.
 *
 * <p>This suite performs end-to-end validation of the storage layer's interaction
 * with an S3-compatible backend (MinIO). It focuses on verifying data persistence,
 * resilient retrieval, and the critical byte-level synchronization logic used
 * to align records across distributed chunk boundaries.</p>
 *
 * <p><b>Execution Prerequisites:</b><br>
 * This test requires an active MinIO instance accessible at {@code http://localhost:9000}.
 * Ensure the environment is provisioned via {@code docker-compose up -d} prior to execution.</p>
 *
 * <p><b>Fault Tolerance Note:</b><br>
 * Methods are configured with {@link Throwable} signatures to accommodate the
 * {@code Resilience4j} retry wrappers implemented in the service layer.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 * @see com.iliasbolan.services.S3ClientService
 */
class S3ClientServiceIntegrationTest {

    /** The service under test, injected with a local connection manager. */
    private static S3ClientService s3ClientService;

    /** The bucket used exclusively for integration testing. */
    private static final String TEST_BUCKET = "integration-test-bucket";

    /**
     * Bootstraps the local integration environment and infrastructure dependencies.
     *
     * <p>This setup phase performs the following operations:
     * <ol>
     * <li>Establishes a connection to the Docker-hosted MinIO instance.</li>
     * <li>Initializes the {@link S3ClientService} with the connection manager.</li>
     * <li>Ensures the {@code TEST_BUCKET} exists, creating it if necessary to
     * guarantee a clean state for subsequent tests.</li>
     * </ol>
     * </p>
     *
     * @throws Exception If connectivity to the MinIO container fails or bucket
     * provisioning is interrupted.
     */
    @BeforeAll
    static void setUpAll() throws Exception {
        // 1. Connect to the local Docker MinIO instance using your standard defaults
        MinioConnectionManager manager = new MinioConnectionManager(
                "http://localhost:9000",
                "minioadmin",
                "minioadmin"
        );
        s3ClientService = new S3ClientService(manager);

        // 2. Ensure the test bucket exists before we try to write to it
        MinioClient client = manager.getClient();
        boolean bucketExists = client.bucketExists(io.minio.BucketExistsArgs.builder().bucket(TEST_BUCKET).build());
        if (!bucketExists) {
            client.makeBucket(MakeBucketArgs.builder().bucket(TEST_BUCKET).build());
        }
    }

    /**
     * Validates a complete Write/Read round-trip and record boundary synchronization.
     *
     * <p>This test specifically targets the <b>Boundary Correction Algorithm</b>.
     * By providing an intentional byte offset that starts in the middle of a record,
     * the test verifies that the service:
     * <ul>
     * <li>Successfully skips the leading partial record (Sync Phase).</li>
     * <li>Identifies the correct start of the next complete record via newline
     * detection (0x0A).</li>
     * <li>Parses subsequent data correctly into clean UTF-8 strings.</li>
     * </ul>
     * </p>
     *
     * @throws Throwable To accommodate service-layer retry mechanisms and
     * I/O exceptions.
     */
    @Test
    void testWriteAndReadDataChunk_SuccessfulRoundTrip() throws Throwable {
        // Arrange: Use newlines so the Record Reader has boundaries to work with.
        // In UTF-8, these characters are 1 byte each (Indices 0-11 for A-F).
        String objectName = "test-job/chunk-test.txt";
        String testData = "A\nB\nC\nD\nE\nF\nG\nH\nI\nJ\nK\nL";

        // Act 1: Write the data to MinIO (Now resilient via Resilience4j)
        s3ClientService.writeData(TEST_BUCKET, objectName, testData);

        // Act 2: Read the chunk.
        // SYNC LOGIC: We set offset to 8 (pointing at 'E').
        // The service will read 'E' (8), then '\n' (9), and then start 'F' at index 10.
        List<String> records = s3ClientService.readDataChunk(TEST_BUCKET, objectName, 8, 20);

        // Assert
        assertNotNull(records);
        assertFalse(records.isEmpty(), "No records were parsed from the chunk!");

        // Check if the first clean record after our offset/skip logic is "F"
        assertEquals("F", records.get(0), "The first record did not match expected alignment!");
    }
}