package com.iliasbolan.storage;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * A service class responsible for all direct communications between the Worker node
 * and the Shared File System (MinIO).
 * <p>
 * In this Map-Reduce architecture, Workers bypass the Manager Service for data
 * transfer to avoid network congestion. This class utilizes the MinIO Java SDK
 * to perform idempotent writes, fetch specific byte-ranges of input files (chunks),
 * and download user-provided execution code.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.1
 * @since 2026-03-30
 */
public class S3ClientService {

    // Instantiate the SLF4J Logger specific to this class
    private static final Logger logger = LoggerFactory.getLogger(S3ClientService.class);

    private final MinioClient minioClient;

    /**
     * Initializes the S3 Client connection to the MinIO server.
     *
     * @param endpoint  The URL of the MinIO server (e.g., "http://localhost:9000").
     * @param accessKey The root user or access key.
     * @param secretKey The root password or secret key.
     */
    public S3ClientService(String endpoint, String accessKey, String secretKey) {
        this.minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(accessKey, secretKey)
                .build();

        logger.info("Initialized S3ClientService. Connected to MinIO endpoint: {}", endpoint);
    }

    /**
     * Downloads the user's compiled Java code (.class or .jar) from MinIO to the
     * Worker's local file system so it can be dynamically loaded via Reflection.
     *
     * @param bucketName      The name of the bucket where the code resides.
     * @param objectName      The S3 object key (e.g., "jobs/job_1/MyMapper.class").
     * @param destinationPath The local directory path where the file should be saved.
     * @throws Exception If a network error occurs or the file cannot be written.
     */
    public void downloadUserCode(String bucketName, String objectName, String destinationPath) throws Exception {
        Path targetPath = Paths.get(destinationPath);
        logger.info("Downloading user code from s3://{}/{} to local path: {}", bucketName, objectName, destinationPath);

        try {
            // Ensure the parent directories exist
            Files.createDirectories(targetPath.getParent());

            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .build())) {

                // Copy the S3 stream directly to the local file system
                Files.copy(stream, targetPath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Successfully downloaded user code to: {}", targetPath.toAbsolutePath());
            }
        } catch (Exception e) {
            logger.error("Failed to download user code from s3://{}/{}", bucketName, objectName, e);
            throw e;
        }
    }

    /**
     * Reads a specific chunk of data from a large input file.
     * <p>
     * Rather than downloading the entire file, this method requests a specific byte-range.
     * This is essential for the Map phase, where the Manager assigns 64MB chunks
     * to individual workers for parallel processing.
     * </p>
     *
     * @param bucketName The name of the bucket containing the input data.
     * @param objectName The S3 object key of the input file.
     * @param offset     The starting byte position of the chunk.
     * @param length     The total number of bytes to read (e.g., 67108864 for 64MB).
     * @return A {@link String} containing the raw text data of the chunk.
     * @throws Exception If the byte-range request fails.
     */
    public String readDataChunk(String bucketName, String objectName, long offset, long length) throws Exception {
        logger.debug("Requesting data chunk from s3://{}/{}. Offset: {}, Length: {} bytes", bucketName, objectName, offset, length);

        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .offset(offset)
                        .length(length)
                        .build())) {

            // Convert the input stream to a String
            String chunkData = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            logger.debug("Successfully read data chunk from MinIO.");
            return chunkData;

        } catch (Exception e) {
            logger.error("Failed to read data chunk from s3://{}/{}. Offset: {}, Length: {}", bucketName, objectName, offset, length, e);
            throw e;
        }
    }

    /**
     * Writes intermediate Map output or final Reduce output back to MinIO.
     * <p>
     * To ensure Idempotence (At-Least-Once delivery semantics), the objectName
     * must be deterministic (e.g., "s3://bucket/job_1/intermediate/map_1_part_0.json").
     * If this method is called multiple times for the same task due to a Worker crash,
     * it will safely overwrite the existing object without data corruption.
     * </p>
     *
     * @param bucketName The destination bucket.
     * @param objectName The deterministic S3 object key for the output file.
     * @param data       The JSON or text data to be written.
     * @throws Exception If the upload fails.
     */
    public void writeData(String bucketName, String objectName, String data) throws Exception {
        byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
        logger.debug("Writing {} bytes to s3://{}/{}", dataBytes.length, bucketName, objectName);

        try (InputStream inputStream = new ByteArrayInputStream(dataBytes)) {
            minioClient.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .stream(inputStream, dataBytes.length, -1)
                            .contentType("application/json") // Adjust depending on your final format
                            .build());

            logger.debug("Successfully wrote object to s3://{}/{}", bucketName, objectName);
        } catch (Exception e) {
            logger.error("Failed to write data to s3://{}/{}", bucketName, objectName, e);
            throw e;
        }
    }
}