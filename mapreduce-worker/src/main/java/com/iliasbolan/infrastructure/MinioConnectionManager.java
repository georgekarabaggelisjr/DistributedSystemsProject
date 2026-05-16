package com.iliasbolan.infrastructure;

import com.iliasbolan.services.S3ClientService;
import io.minio.MinioClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle and configuration of the MinIO (S3-compatible) client connection.
 *
 * <p><b>Design Principles:</b><br>
 * This class serves as a concrete implementation of the <i>Single Responsibility Principle (SRP)</i>
 * by centralizing the initialization and configuration of the storage client. By isolating the
 * connection logic, the system ensures that credentials—such as access keys and secret
 * passwords—remain abstracted from the business logic layer, reducing the risk of
 * sensitive data exposure across the application context.</p>
 *
 * <p><b>Thread Safety:</b><br>
 * The underlying {@link MinioClient} is designed to be thread-safe. This manager maintains
 * a single instance of the client, making it suitable for injection into multithreaded
 * services such as the {@link S3ClientService}.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-03-30
 * @see io.minio.MinioClient
 */
public class MinioConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(MinioConnectionManager.class);

    /** The pre-configured client used for executing all S3 operations. */
    private final MinioClient minioClient;

    /**
     * Initializes the S3-compatible client connection to the specified MinIO server.
     *
     * <p>Utilizes the Builder pattern provided by the MinIO SDK to establish a
     * persistent client instance with the provided endpoint and security credentials.</p>
     *
     * @param endpoint  The URL endpoint of the MinIO server (e.g., http://minio:9000).
     * @param accessKey The root user identifier or access key for authentication.
     * @param secretKey The root password or secret key for authentication.
     */
    public MinioConnectionManager(String endpoint, String accessKey, String secretKey) {
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        // Security Compliance: Do not log the secret key.
        logger.info("MinioConnectionManager initialized securely for endpoint: {}", endpoint);
    }

    /**
     * Provides the configured {@code MinioClient} instance for executing storage operations.
     *
     * <p>The returned client is the primary entry point for interactions with the
     * S3 bucket architecture, including object retrieval, metadata scanning,
     * and data persistence.</p>
     *
     * @return A ready-to-use {@link MinioClient} instance.
     */
    public MinioClient getClient() {
        return this.minioClient;
    }
}