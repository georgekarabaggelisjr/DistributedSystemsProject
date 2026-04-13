package com.iliasbolan.storage;

import io.minio.*;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

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
 * @version 1.2
 * @since 2026-03-30
 */
public class S3ClientService {

    private static final Logger logger = LoggerFactory.getLogger(S3ClientService.class);

    private final MinioClient minioClient;

    /**
     * Initializes the service with a pre-configured MinIO connection.
     *
     * @param connectionManager The manager providing the secure MinioClient.
     */
    public S3ClientService(MinioConnectionManager connectionManager) {
        this.minioClient = connectionManager.getClient();
        logger.info("Initialized S3ClientService using injected MinioConnectionManager.");
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
            Files.createDirectories(targetPath.getParent());

            try (InputStream stream = minioClient.getObject(
                    GetObjectArgs.builder()
                            .bucket(bucketName)
                            .object(objectName)
                            .build())) {

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
                            .contentType("application/json")
                            .build());

            logger.debug("Successfully wrote object to s3://{}/{}", bucketName, objectName);
        } catch (Exception e) {
            logger.error("Failed to write data to s3://{}/{}", bucketName, objectName, e);
            throw e;
        }
    }

    /**
     * Scans the intermediate storage directory to identify all partition fragments
     * produced by various Map tasks that belong to a specific Reducer.
     * <p>
     * This method is essential for the Shuffle/Sort boundary. It looks for files
     * following the deterministic naming convention: <code>[jobId]/intermediate/*_part_[index].txt</code>.
     * </p>
     *
     * @param bucketName     The MinIO bucket containing intermediate job data.
     * @param jobId          The unique identifier for the current job.
     * @param partitionIndex The specific partition (0 to R-1) this worker is assigned to reduce.
     * @return A {@link List} of S3 object keys representing the fragments to be reduced.
     * @throws Exception If a network error occurs during the listing process.
     */
    public List<String> listIntermediateFiles(String bucketName, String jobId, int partitionIndex) throws Exception {
        String prefix = jobId + "/intermediate/";
        String suffix = "_part_" + partitionIndex + ".txt";
        List<String> matchingObjects = new ArrayList<>();

        // We use a recursive listing to capture all map outputs regardless of sub-folder structure
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

        logger.info("Discovery phase complete. Found {} intermediate fragments for partition {} in job {}.",
                matchingObjects.size(), partitionIndex, jobId);
        return matchingObjects;
    }

    /**
     * Downloads and reads an entire S3 object into memory as a UTF-8 String.
     * <p>
     * <b>Note:</b> This is intended for intermediate files which are typically
     * significantly smaller than the original 64MB input chunks.
     * </p>
     *
     * @param bucketName The source MinIO bucket.
     * @param objectName The deterministic S3 object key.
     * @return The raw text content of the intermediate file.
     * @throws Exception If the file cannot be retrieved or read.
     */
    public String readObject(String bucketName, String objectName) throws Exception {
        try (InputStream stream = minioClient.getObject(
                GetObjectArgs.builder().bucket(bucketName).object(objectName).build())) {

            return new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}