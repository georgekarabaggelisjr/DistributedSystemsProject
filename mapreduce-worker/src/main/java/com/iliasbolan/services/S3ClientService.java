package com.iliasbolan.services;

import com.iliasbolan.infrastructure.MinioConnectionManager;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.minio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
 * <h3>Key Architectural Features:</h3>
 * <ul>
 * <li><b>Multi-Tenant Compatibility:</b> Fully decoupled from hardcoded paths, natively supporting
 * isolated user sandbox environments (e.g., <code>s3://data/&lt;user_id&gt;/...</code>).</li>
 * <li><b>Resilience:</b> Implements {@code Resilience4j} retries with exponential backoff
 * and random jitter to survive transient network partitions.</li>
 * <li><b>Zero-Loss Boundary Correction (Lookback Pattern):</b> Implements an advanced
 * <code>offset - 1</code> lookback sync to guarantee that perfectly aligned chunks do not
 * erroneously drop their first record, eliminating silent data loss across parallel maps.</li>
 * <li><b>Dynamic Memory Eviction:</b> Utilizes array pre-allocation, buffered streams, and
 * dynamic byte-buffer re-instantiation to prevent permanent heap hoarding on malformed lines,
 * guaranteeing stable heap usage and preventing K8s OOMKilled container terminations.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 3.2
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
     * @param bucketName      S3 bucket containing the code artifacts (e.g., "code").
     * @param objectName      Sandboxed S3 key for the bytecode (e.g., "&lt;user_id&gt;/Mapper.class").
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
     * <b>Algorithm Detail (Hadoop Lookback Pattern):</b>
     * To support parallel processing without dropping data, this method evaluates the exact
     * byte prior to the designated chunk start:
     * <ol>
     * <li>If the byte at <code>offset - 1</code> is NOT a newline (0x0A), the chunk starts
     * mid-record. It discards the leading fragment until the first newline is reached.</li>
     * <li>If the byte at <code>offset - 1</code> IS a newline, the chunk is perfectly aligned
     * and processing begins immediately, preventing the silent deletion of the first valid record.</li>
     * </ol>
     * </p>
     * <p>
     * <b>Security Note:</b> Incorporates an absolute 100MB line-limit guard, alongside dynamic
     * 1MB buffer eviction to protect the JVM Heap from permanently hoarding RAM on malformed datasets.
     * </p>
     *
     * @param bucketName Target S3 bucket (e.g., "data").
     * @param objectName Sandboxed S3 key of the source file (e.g., "&lt;user_id&gt;/input.txt").
     * @param offset     The logical starting byte (from the Manager).
     * @param length     The target chunk size (typically 64MB - 128MB).
     * @return A {@link List} of UTF-8 encoded, sanitized text records.
     * @throws Throwable if the stream is interrupted, decoding fails, or the OOM guard is triggered.
     */
    public List<String> readDataChunk(String bucketName, String objectName, long offset, long length) throws Throwable {
        return Retry.decorateCheckedSupplier(retryContext, () -> {
            logger.info(" Offset: {}, Target Length: {} bytes", offset, length);

            int estimatedRecordCount = (int) (length / 100);
            List<String> cleanRecords = new ArrayList<>(estimatedRecordCount);

            long fetchOffset = offset > 0 ? offset - 1 : 0;
            long fetchLength = offset > 0 ? length + 1 : length;

            // --- The Stream Truncation Bug ---
            // Removed .length(fetchLength) from the GetObjectArgs builder.
            // This allows the stream to naturally read past the mathematical chunk boundary
            // to finish the final word without MinIO abruptly severing the TCP connection.
            try (InputStream rawStream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .offset(fetchOffset)
                            .build());
                 java.io.BufferedInputStream stream = new java.io.BufferedInputStream(rawStream, 32768)) {
                // ----------------------------------------

                long bytesProcessedInChunk = 0;
                boolean skipFirstRecord = false;

                if (offset > 0) {
                    int lookbackByte = stream.read();
                    bytesProcessedInChunk++;
                    if (lookbackByte != 0x0A) {
                        skipFirstRecord = true;
                    }
                }

                if (skipFirstRecord) {
                    int b;
                    while ((b = stream.read()) != -1) {
                        bytesProcessedInChunk++;
                        if (b == 0x0A) break;
                    }
                }

                ByteArrayOutputStream lineBuffer = new ByteArrayOutputStream(256);
                final int MAX_LINE_SIZE_BYTES = 100 * 1024 * 1024;
                int b;

                while ((b = stream.read()) != -1) {
                    bytesProcessedInChunk++;
                    lineBuffer.write(b);

                    if (lineBuffer.size() > MAX_LINE_SIZE_BYTES) {
                        throw new IOException(
                                String.format("Security/OOM Guard Triggered: Encountered a line exceeding %d bytes.", MAX_LINE_SIZE_BYTES)
                        );
                    }

                    if (b == 0x0A) {
                        String line = lineBuffer.toString(StandardCharsets.UTF_8);

                        // --- Semantic Whitespace Preservation ---
                        // Replaces .trim() with targeted boundary character stripping.
                        // This removes Windows (\r) and Unix (\n) line endings while safely
                        // preserving inner tabs and spaces required by the Mapper.
                        if (line.endsWith("\r\n")) {
                            line = line.substring(0, line.length() - 2);
                        } else if (line.endsWith("\n")) {
                            line = line.substring(0, line.length() - 1);
                        } else if (line.endsWith("\r")) {
                            line = line.substring(0, line.length() - 1);
                        }
                        // -----------------------------------------------

                        if (!line.isEmpty()) {
                            cleanRecords.add(line);
                        }

                        if (lineBuffer.size() > 1024 * 1024) {
                            lineBuffer = new ByteArrayOutputStream(256);
                        } else {
                            lineBuffer.reset();
                        }

                        // The loop naturally terminates here, gracefully closing the streams
                        if (bytesProcessedInChunk >= fetchLength) {
                            logger.info("Fulfilled quota ({} bytes). Closing stream.", bytesProcessedInChunk);
                            break;
                        }
                    }
                }

                // Handle file trailing bytes missing a newline
                if (lineBuffer.size() > 0) {
                    String lastLine = lineBuffer.toString(StandardCharsets.UTF_8);

                    if (lastLine.endsWith("\r")) {
                        lastLine = lastLine.substring(0, lastLine.length() - 1);
                    }

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
     * Persists final computational output to the shared storage layer.
     * <p>
     * In the modern P2P architecture, intermediate shuffle data is stored on local disk
     * (ESS) rather than S3. Therefore, this method is now exclusively utilized by Reducers
     * to persist the final, aggregated outputs of the job. Data is encoded in UTF-8.
     * </p>
     *
     * @param bucketName  Target S3 bucket (e.g., "results").
     * @param objectName  Sandboxed path (e.g., "&lt;user_id&gt;/&lt;job_id&gt;/part_n.txt").
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
                                .contentType("text/plain")
                                .build());

                logger.debug("Successfully wrote object to s3://{}/{}", bucketName, objectName);
            }
        }).run();
    }

    /**
     * Streams a local file directly to S3, bypassing the JVM Heap.
     * <p>
     * <b>PERFORMANCE OPTIMIZATION:</b> Enforces O(1) memory footprint during massive
     * final Reduce uploads by streaming directly from the local disk buffer to the network.
     * Utilizing MinIO's optimized uploader allows for under-the-hood multipart parallel uploads.
     * </p>
     *
     * @param bucketName Target S3 bucket (e.g., "results").
     * @param objectName Sandboxed path (e.g., "&lt;user_id&gt;/&lt;job_id&gt;/part_n.txt").
     * @param filePath   The local physical file to be uploaded.
     * @throws Throwable if the upload fails or the disk is unreadable.
     */
    public void uploadFileFromDisk(String bucketName, String objectName, Path filePath) throws Throwable {
        Retry.decorateCheckedRunnable(retryContext, () -> {
            logger.debug("Streaming file {} to s3://{}/{}", filePath, bucketName, objectName);

            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .filename(filePath.toAbsolutePath().toString())
                            .contentType("text/plain")
                            .build());

            logger.debug("Successfully streamed file to S3.");
        }).run();
    }
}