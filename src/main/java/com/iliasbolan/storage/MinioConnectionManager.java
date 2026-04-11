package com.iliasbolan.storage;

import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the connection configuration for the MinIO (S3) server.
 * <p>
 * By extracting the connection logic into this dedicated manager, we adhere to the
 * Single Responsibility Principle. This ensures the application can request a
 * pre-configured MinioClient without exposing access keys or secret passwords
 * to the business logic layer.
 * </p>
 */
public class MinioConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MinioConnectionManager.class);
    private final MinioClient minioClient;

    /**
     * Initializes the S3 Client connection to the MinIO server.
     *
     * @param endpoint  The URL of the MinIO server.
     * @param accessKey The root user or access key.
     * @param secretKey The root password or secret key.
     */
    public MinioConnectionManager(String endpoint, String accessKey, String secretKey) {
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        // We do NOT log the secret key!
        logger.info("MinioConnectionManager initialized securely for endpoint: {}", endpoint);
    }

    /**
     * Provides the configured MinioClient for executing S3 operations.
     *
     * @return A ready-to-use {@link MinioClient}.
     */
    public MinioClient getClient() {
        return this.minioClient;
    }
}