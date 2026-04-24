package com.iliasbolan.services;

import com.iliasbolan.infrastructure.MinioConnectionManager;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.minio.*;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

/**
 * High-performance I/O service for distributed storage orchestration.
 * <p>
 * This service encapsulates all interactions between the Worker node and the S3-compatible
 * storage layer (MinIO). It is engineered to handle massive data throughput while
 * maintaining strict record-level consistency during parallel file reads.
 * </p>
 * * <h3>Key Architectural Features:</h3>
 * <ul>
 * <li><b>Resilience:</b> Implements {@code Resilience4j} retries with exponential backoff
 * and random jitter to survive transient network partitions.</li>
 * <li><b>Boundary Correction:</b> Implements a specialized synchronization algorithm
 * to ensure UTF-8 records are never bifurcated across Map chunks.</li>
 * <li><b>Idempotent Operations:</b> Ensures that task retries do not result in corrupted
 * or duplicated intermediate data.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @see <a href="https://resilience4j.readme.io/">Resilience4j Documentation</a>
 * @since 2026-03-30
 */
public class S3ClientService {

    /** Logger for storage events and retry lifecycle tracking. */
    private static final Logger logger = LoggerFactory.getLogger(S3ClientService.class);

    /** The underlying MinIO client for protocol-level communication. */
    private final MinioClient minioClient;

    /** Resilience4j context for managing retry state and backoff intervals. */
    private final Retry retryContext;

    /**
     * Constructs the service and configures the fault-tolerance registry.
     * <p>
     * The retry policy is configured for 3 attempts with an exponential random
     * backoff (Base: 1s, Multiplier: 2.0).
     * </p>
     *
     * @param connectionManager Provider for the authenticated MinioClient.
     */
    public S3ClientService(MinioConnectionManager connectionManager) {
        this.minioClient = connectionManager.getClient();

        // Fault tolerance configuration
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(1000, 2.0, 0.5))
                .retryExceptions(Exception.class)
                .build();

        this.retryContext = RetryRegistry.of(config).retry("minio-communication-retry");
        logger.info("Initialized S3ClientService with Resilience4j Fault-Tolerance.");
    }

    /**
     * Downloads executable bytecode from the shared storage to the local runtime environment.
     * <p>
     * This is a critical step in the worker lifecycle, enabling the dynamic loading of
     * user-provided {@code .class} or {@code .jar} files via the {@code TaskExecutor}.
     * </p>
     *
     * @param bucketName      S3 bucket containing the code artifacts.
     * @param objectName      S3 key for the specific bytecode file.
     * @param destinationPath Local file system path for temporary storage.
     * @throws Throwable if the artifact cannot be retrieved or the local disk is write-protected.
     */
    public void downloadUserCode(String bucketName, String objectName, String destinationPath) throws Throwable {
        Retry.decorateCheckedRunnable(retryContext, () -> {
            Path targetPath = Paths.get(destinationPath);
            logger.info("Downloading user code from s3://{}/{} to local path: {}", bucketName, objectName, destinationPath);

            Files.createDirectories(targetPath.getParent());

            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .build())) {

                Files.copy(stream, targetPath, StandardCopyOption.REPLACE_EXISTING);
                logger.info("Successfully downloaded user code to: {}", targetPath.toAbsolutePath());
            }
        }).run();
    }

    /**
     * Reads a boundary-corrected chunk of data from a large file.
     * <p>
     * <b>Algorithm Detail:</b>
     * To support parallel processing of a single large file, this method implements
     * "Record Synchronization":
     * <ol>
     * <li>If the chunk starts mid-file (offset > 0), it discards the leading fragment
     * until the first newline (0x0A) is reached.</li>
     * <li>It reads the requested {@code length}, but continues reading until the
     * current line is finalized.</li>
     * </ol>
     * This ensures the <i>preceding</i> worker handles the fragment we skipped,
     * and <i>we</i> handle the fragment that the <i>following</i> worker will skip.
     * </p>
     *
     * @param bucketName S3 bucket containing the input data.
     * @param objectName S3 key of the source file.
     * @param offset     The logical starting byte (from the Manager).
     * @param length     The target chunk size (typically 64MB).
     * @return A {@link List} of UTF-8 encoded, sanitized text records.
     * @throws Throwable if the stream is interrupted or the data cannot be decoded.
     */
    public List<String> readDataChunk(String bucketName, String objectName, long offset, long length) throws Throwable {
        return Retry.decorateCheckedSupplier(retryContext, () -> {
            logger.info(" Offset: {}, Target Length: {} bytes", offset, length);

            List<String> cleanRecords = new ArrayList<>();

            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .offset(offset)
                            .build())) {

                // Move the counter to track skipped bytes
                long bytesProcessedInChunk = 0;

                // SYNC PHASE: Seek to the first valid record start (newline)
                if (offset > 0) {
                    int b;
                    while ((b = stream.read()) != -1) {
                        bytesProcessedInChunk++; // Count skipped bytes!
                        if (b == 0x0A) break;
                    }
                }

                // EXTRACTION PHASE: Read target length + finish current line
                ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream();
                int b;

                while ((b = stream.read()) != -1) {
                    bytesProcessedInChunk++; // Continue counting
                    lineBuffer.write(b);

                    // UTF-8 records are separated by standard LF (0x0A)
                    if (b == 0x0A) {
                        String line = lineBuffer.toString(StandardCharsets.UTF_8).trim();

                        if (!line.isEmpty()) {
                            cleanRecords.add(line);
                        }
                        lineBuffer.reset();

                        // Termination condition: Quota reached AND record completed
                        if (bytesProcessedInChunk >= length) {
                            logger.info("Fulfilled quota ({} bytes). Closing stream.", bytesProcessedInChunk);
                            break;
                        }
                    }
                }

                // Edge Case: Handle file trailing bytes missing a newline
                if (lineBuffer.size() > 0) {
                    String lastLine = lineBuffer.toString(StandardCharsets.UTF_8).trim();
                    if (!lastLine.isEmpty()) {
                        cleanRecords.add(lastLine);
                    }
                }

                logger.info("Processing complete. Records parsed: {}", cleanRecords.size());
                return cleanRecords;
            }
        }).get();
    }

    /**
     * Persists computational output to the shared storage layer.
     * <p>
     * This is used for both intermediate Map partitions and final Reduce results.
     * Data is encoded in UTF-8 to maintain character set consistency across the cluster.
     * </p>
     *
     * @param bucketName  Target S3 bucket.
     * @param objectName  Deterministic path (e.g., job_id/intermediate/part_n.txt).
     * @param data        Raw text results to be uploaded.
     * @throws Throwable if the upload is rejected by the storage cluster.
     */
    public void writeData(String bucketName, String objectName, String data) throws Throwable {
        Retry.decorateCheckedRunnable(retryContext, () -> {
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            logger.debug("Writing {} bytes to s3://{}/{}", dataBytes.length, bucketName, objectName);

            try (InputStream inputStream = new ByteArrayInputStream(dataBytes)) {
                minioClient.putObject(
                        PutObjectArgs.builder()
                                .bucket(bucketName)
                                .object(objectName)
                                .stream(inputStream, dataBytes.length, -1)
                                .contentType("application/json")
                                .build());

                logger.debug("Successfully wrote object to s3://{}/{}", bucketName, objectName);
            }
        }).run();
    }

    /**
     * Discovers all intermediate fragments associated with a specific Reduce partition.
     * <p>
     * This is the entry point for the <b>Shuffle phase</b>. It performs a recursive
     * search for all files matching the partition-specific suffix created by
     * the various Map tasks.
     * </p>
     *
     * @param bucketName     Bucket containing intermediate results.
     * @param jobId          UUID of the active job.
     * @param partitionIndex The specific partition (Modulo ID) the worker is reducing.
     * @return A {@link List} of object keys ready for ingestion.
     * @throws Throwable if listing permissions are denied or network fails.
     */
    @SuppressWarnings("unused")
    public List<String> listIntermediateFiles(String bucketName, String jobId, int partitionIndex) throws Throwable {
        return Retry.decorateCheckedSupplier(retryContext, () -> {
            String prefix = jobId + "/intermediate/";
            String suffix = "_part_" + partitionIndex + ".txt";
            List<String> matchingObjects = new ArrayList<>();

            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucketName)
                            .prefix(prefix)
                            .recursive(true)
                            .build());

            for (Result<Item> result : results) {
                String name = result.get().objectName();
                if (name.endsWith(suffix)) {
                    matchingObjects.add(name);
                }
            }

            logger.info("Found {} intermediate fragments for partition {} in job {}.",
                    matchingObjects.size(), partitionIndex, jobId);
            return matchingObjects;
        }).get();
    }

    /**
     * Retrieves an entire S3 object as a UTF-8 String.
     * <p>
     * <b>Warning:</b> This method loads the entire object into memory. It is suitable
     * for intermediate partitions but should not be used for raw input files.
     * </p>
     *
     * @param bucketName Source bucket.
     * @param objectName Specific object key.
     * @return The full text content of the file.
     * @throws Throwable if the object is too large for memory or retrieval fails.
     */
    @SuppressWarnings("unused")
    public String readObject(String bucketName, String objectName) throws Throwable {
        return Retry.decorateCheckedSupplier(retryContext, () -> {
            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder().bucket(bucketName).object(objectName).build())) {

                return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            }
        }).get();
    }
}