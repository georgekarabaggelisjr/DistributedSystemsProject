package com.iliasbolan.storage;

import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
        // Arrange: Use newlines so the Record Reader has boundaries to work with.
        // In UTF-16, each character (including \n) is 2 bytes.
        String objectName = "test-job/chunk-test.txt";
        String testData = "A\nB\nC\nD\nE\nF\nG\nH\nI\nJ\nK\nL";

        // Act 1: Write the data to MinIO
        s3ClientService.writeData(TEST_BUCKET, objectName, testData);

        // Act 2: Read the chunk.
        // Note: Rule #1 will skip everything until the first \n AFTER the offset.
        // If we want to start at 'F', we set the offset to point into 'E'.
        List<String> records = s3ClientService.readDataChunk(TEST_BUCKET, objectName, 18, 20);

        // Assert
        assertNotNull(records);
        assertFalse(records.isEmpty(), "No records were parsed from the chunk!");

        // Check if the first clean record after our offset/skip logic is "F"
        assertEquals("F", records.get(0), "The first record did not match expected alignment!");
    }
}