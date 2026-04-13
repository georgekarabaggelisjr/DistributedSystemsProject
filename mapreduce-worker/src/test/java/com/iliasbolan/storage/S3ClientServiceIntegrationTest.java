package com.iliasbolan.storage;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Integration tests for the {@link S3ClientService}.
 * <p>
 * IMPORTANT: This test requires a live MinIO instance running on localhost:9000.
 * Run 'docker-compose up -d' before executing this class.
 * </p>
 */
class S3ClientServiceIntegrationTest {

    private static S3ClientService s3ClientService;
    private static final String TEST_BUCKET = "integration-test-bucket";

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

    @Test
    void testWriteAndReadDataChunk_SuccessfulRoundTrip() throws Exception {
        // Arrange
        String objectName = "test-job/chunk-test.txt";
        // Write exactly 26 bytes (the alphabet)
        String testData = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        // Act 1: Write the data to MinIO
        s3ClientService.writeData(TEST_BUCKET, objectName, testData);

        // Act 2: Read only a specific "chunk" back.
        // Ask for 5 bytes starting at offset 5. (Should return "FGHIJ")
        String chunk = s3ClientService.readDataChunk(TEST_BUCKET, objectName, 5, 5);

        // Assert
        assertNotNull(chunk);
        assertEquals("FGHIJ", chunk, "The byte-range chunk downloaded from MinIO did not match the expected offset!");
    }
}