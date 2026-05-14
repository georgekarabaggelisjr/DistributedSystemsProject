package com.iliasbolan.ess.storage;

import io.minio.*;
import io.minio.messages.Item;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * Enterprise-grade MinIO Storage adapter for the External Shuffle Service (ESS) overflow tier.
 * <p>
 * This service manages the seamless offloading (spilling) of intermediate MapReduce shuffle
 * data to object storage when physical node disk utilization reaches critical thresholds.
 * By persisting state to an S3-compatible backend, it safely decouples storage from compute,
 * allowing KEDA-driven ephemeral job pods to scale down or terminate in multitenant environments
 * without causing data loss for lagging Reducers.
 * </p>
 * <p>
 * <b>Performance Architecture:</b><br>
 * Utilizes direct physical-to-network streaming for O(1) memory OOM protection during massive
 * spills, and employs parallel concurrent streams during restoration to maximize bandwidth
 * saturation and minimize Reducer wait times.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-05-13
 */
public class MinioStorageService {

    private static final Logger logger = LoggerFactory.getLogger(MinioStorageService.class);

    private final MinioClient minioClient;
    private final String bucketName;

    /**
     * Initializes the MinIO Storage client using environment-injected configurations.
     * Ensures the target bucket exists upon startup, adhering to fail-fast principles.
     *
     * @throws RuntimeException If connection to the MinIO cluster fails or credentials are invalid.
     */
    public MinioStorageService() {
        String endpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://minio:9000");
        String accessKey = System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin");
        String secretKey = System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin");
        this.bucketName = System.getenv().getOrDefault("MINIO_BUCKET", "ess-overflow");

        try {
            this.minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();

            ensureBucketExists();
            logger.info("MinioStorageService initialized successfully connected to endpoint: {}", endpoint);
        } catch (Exception e) {
            logger.error("FATAL: Failed to initialize MinIO Client for overflow tier.", e);
            throw new RuntimeException("MinIO Initialization Failure", e);
        }
    }

    /**
     * Recursively uploads all compressed (.lz4) partition files within a specific Job directory
     * to the MinIO overflow bucket.
     * <p>
     * This operation streams the files directly from the physical disk to the network interface,
     * maintaining a near O(1) memory footprint regardless of directory size.
     * </p>
     *
     * @param jobId        The unique identifier for the MapReduce job.
     * @param jobDirectory The root local path containing the partitioned data for the job.
     * @throws IOException If local disk traversal or network transmission fails.
     */
    public void spillJobData(String jobId, Path jobDirectory) throws IOException {
        if (!Files.exists(jobDirectory)) {
            logger.warn("Attempted to spill non-existent job directory: {}", jobDirectory);
            return;
        }

        logger.info("Initiating MinIO overflow spill for Job: {}", jobId);

        Files.walkFileTree(jobDirectory, new SimpleFileVisitor<>() {
            @Override
            public @NonNull FileVisitResult visitFile(@NonNull Path file, @NonNull BasicFileAttributes attrs) throws IOException {
                if (file.toString().endsWith(".lz4")) {
                    // Calculate relative path to maintain partition structure in the bucket (e.g., job-123/0/part.lz4)
                    String objectName = jobDirectory.getParent().relativize(file).toString().replace("\\", "/");

                    try {
                        minioClient.uploadObject(
                                UploadObjectArgs.builder()
                                        .bucket(bucketName)
                                        .object(objectName)
                                        .filename(file.toAbsolutePath().toString())
                                        .build()
                        );
                        logger.debug("Successfully spilled object: {}", objectName);
                    } catch (Exception e) {
                        logger.error("Failed to spill file {} to MinIO.", file, e);
                        throw new IOException("MinIO spill operation interrupted", e);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        logger.info("Spill complete for Job: {}", jobId);
    }

    /**
     * Restores all partition segments from the MinIO overflow bucket back to the physical disk.
     * <p>
     * <b>Concurrent Restoration Logic:</b> Performs a prefix-based scan of the bucket to identify
     * all object segments belonging to the specific Job/Partition hierarchy. It then leverages
     * a parallel stream to download all segments concurrently, saturating the network interface
     * to drastically reduce cache-miss latency for the waiting Reducer.
     * </p>
     *
     * @param jobId       The unique identifier for the MapReduce job.
     * @param partitionId The target partition integer.
     * @param targetDir   The local directory where the partition segments should be restored.
     * @throws IOException If the object listing fails or network/disk I/O is interrupted.
     */
    public void restorePartition(String jobId, int partitionId, Path targetDir) throws IOException {
        // Construct the prefix: e.g., "job-123/0/"
        String objectPrefix = jobId + "/" + partitionId + "/";

        logger.info("Initiating multi-segment concurrent restoration from MinIO for Job: {}, Partition: {}", jobId, partitionId);

        try {
            // Ensure the local directory structure exists
            Files.createDirectories(targetDir);

            // 1. List all objects matching the job/partition prefix
            Iterable<Result<Item>> results = minioClient.listObjects(
                    ListObjectsArgs.builder()
                            .bucket(bucketName)
                            .prefix(objectPrefix)
                            .recursive(true) // Ensure we catch nested segments if they exist
                            .build()
            );

            // Buffer items to enable parallel stream processing
            List<Item> itemsToDownload = new ArrayList<>();
            for (Result<Item> result : results) {
                itemsToDownload.add(result.get());
            }

            int segmentCount = itemsToDownload.size();

            if (segmentCount == 0) {
                logger.warn("Restore requested for Job {} Partition {}, but no objects were found in MinIO prefix: {}",
                        jobId, partitionId, objectPrefix);
                return;
            }

            // 2. Perform highly concurrent downloads via parallel execution
            itemsToDownload.parallelStream().forEach(item -> {
                try {
                    String objectName = item.objectName();

                    // Extract the filename from the object path to preserve naming
                    Path targetFile = targetDir.resolve(Path.of(objectName).getFileName());

                    logger.debug("Downloading segment concurrently: {} -> {}", objectName, targetFile);

                    // downloadObject utilizes internal stream-chunking to maintain O(1) memory overhead
                    minioClient.downloadObject(
                            DownloadObjectArgs.builder()
                                    .bucket(bucketName)
                                    .object(objectName)
                                    .filename(targetFile.toAbsolutePath().toString())
                                    .build()
                    );
                } catch (Exception e) {
                    throw new RuntimeException("Failed to download segment concurrently: " + item.objectName(), e);
                }
            });

            logger.info("Successfully restored {} segments concurrently for Partition {} from overflow tier.", segmentCount, partitionId);

        } catch (Exception e) {
            logger.error("Critical failure during MinIO partition restoration: Job {} Partition {}", jobId, partitionId, e);
            throw new IOException("Failed to restore partition from overflow bucket", e);
        }
    }

    /**
     * Idempotent check to guarantee the target overflow bucket exists on the MinIO cluster.
     * Creates the bucket if it is missing.
     *
     * @throws Exception If bucket verification or creation fails.
     */
    private void ensureBucketExists() throws Exception {
        boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        if (!found) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            logger.info("Created new MinIO overflow bucket: {}", bucketName);
        }
    }
}