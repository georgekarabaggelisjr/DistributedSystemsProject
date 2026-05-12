package com.iliasbolan.ess.gc;

import com.iliasbolan.ess.storage.MinioStorageService;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Intelligent background Garbage Collector (Janitor) for the External Shuffle Service (ESS).
 * <p>
 * This service implements a "Space-Aware" reclamation strategy designed for high-throughput
 * MapReduce environments where disk exhaustion on physical nodes is a critical failure vector.
 * Unlike standard time-based TTL (Time-To-Live) mechanisms, this Janitor monitors real-time
 * disk utilization of the underlying {@link FileStore}.
 * </p>
 * <p>
 * <b>High-Resolution Monitoring (LZ4 Optimization):</b><br>
 * Updated to a 30-second polling interval. In environments processing large (11GB+) payloads,
 * disk usage can escalate rapidly. This higher resolution ensures that the reclamation watermarks
 * are enforced reactively to prevent node-level outages.
 * </p>
 * <p>
 * <b>Reclamation Algorithm (MinIO Overflow Tier):</b><br>
 * The Janitor utilizes a Space-Aware Dual-Watermark protocol combined with an S3-compatible overflow tier:
 * <ul>
 * <li><b>High-Watermark (85%):</b> Trigger point for aggressive reclamation. When breached, the
 * system identifies job directories in a FIFO (First-In, First-Out) manner based on creation timestamps.
 * Before local deletion, the job data is safely spilled to the MinIO object store to ensure zero data loss.</li>
 * <li><b>Low-Watermark (70%):</b> Target state. Reclamation continues until disk utilization
 * reaches this safe buffer.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-05-13
 */
public class StorageJanitor {

    private static final Logger logger = LoggerFactory.getLogger(StorageJanitor.class);

    /**
     * Dedicated single-thread scheduler to prevent Janitor tasks from interfering
     * with gRPC shuffle request handling.
     */
    private static ScheduledExecutorService gcExecutor;

    /**
     * Threshold for initiating aggressive space reclamation (85% utilization).
     */
    private static final double HIGH_WATERMARK_PERCENT = 0.85;

    /**
     * Target threshold for halting space reclamation (70% utilization).
     */
    private static final double LOW_WATERMARK_PERCENT = 0.70;

    /**
     * Absolute maximum age for any job directory before it is forcibly purged (24 hours).
     */
    private static final long HARD_TTL_HOURS = 24;

    /**
     * The MinIO storage service used for spilling data before physical deletion.
     */
    private static MinioStorageService minioStorageService;

    /**
     * Initializes and schedules the background Janitor thread to monitor storage health.
     * <p>
     * The scan interval is reduced to 30 seconds to provide reactive space reclamation
     * for large-scale data spikes associated with 11GB+ shuffle payloads.
     * </p>
     *
     * @param baseShuffleDir The root physical directory containing per-job partition data.
     * @param storageService The MinIO storage client to handle data offloading.
     */
    public static void start(String baseShuffleDir, MinioStorageService storageService) {
        minioStorageService = storageService;
        gcExecutor = Executors.newSingleThreadScheduledExecutor();

        gcExecutor.scheduleAtFixedRate(() -> {
            Path rootPath = Paths.get(baseShuffleDir);
            if (!Files.exists(rootPath)) return;

            try {
                // Accessing NIO FileStore to retrieve physical disk metrics
                FileStore store = Files.getFileStore(rootPath);
                long totalSpace = store.getTotalSpace();
                long usableSpace = store.getUsableSpace();
                long usedSpace = totalSpace - usableSpace;

                double usedPercentage = (double) usedSpace / totalSpace;

                logger.debug("Storage Janitor Scan: Disk Utilization at {}", String.format("%.1f%%", usedPercentage * 100));

                // Trigger aggressive reclamation if High-Watermark is reached
                if (usedPercentage >= HIGH_WATERMARK_PERCENT) {
                    logger.warn("HIGH WATERMARK BREACHED ({}%). Running reactive space reclamation...", Math.round(usedPercentage * 100));
                    reclaimSpace(rootPath, store, totalSpace);
                }

                // Execute the fallback TTL sweep to clean up zombie job data
                purgeHardTtl(rootPath);

            } catch (IOException e) {
                logger.error("Janitor Service failure: {}", e.getMessage());
            }
        }, 10, 30, TimeUnit.SECONDS); // Polling resolution increased to 30 seconds
    }

    /**
     * Gracefully terminates the background Janitor service.
     */
    public static void stop() {
        if (gcExecutor != null && !gcExecutor.isShutdown()) {
            gcExecutor.shutdownNow();
        }
    }

    /**
     * Temporary data structure for O(N) timestamp-based sorting.
     */
    private static class JobDirectory {
        final Path path;
        final long creationTime;

        JobDirectory(Path path, long creationTime) {
            this.path = path;
            this.creationTime = creationTime;
        }
    }

    /**
     * Identifies and purges the oldest job directories until disk usage is restored
     * to the Low-Watermark buffer. Integrates with the MinIO overflow tier to
     * persist data prior to local eviction.
     *
     * @param rootPath   The directory to scan for jobs.
     * @param store      The FileStore being managed.
     * @param totalSpace Total capacity of the store for percentage calculations.
     * @throws IOException If the directory listing or attribute access fails.
     */
    private static void reclaimSpace(Path rootPath, FileStore store, long totalSpace) throws IOException {
        List<Path> sortedJobs;

        // Collect and sort all subdirectories by pre-fetched creation time (Oldest First)
        try (Stream<Path> stream = Files.list(rootPath)) {
            sortedJobs = stream.filter(Files::isDirectory)
                    .map(p -> {
                        try {
                            long time = Files.readAttributes(p, BasicFileAttributes.class).creationTime().toMillis();
                            return new JobDirectory(p, time);
                        } catch (IOException e) {
                            return new JobDirectory(p, Long.MAX_VALUE);
                        }
                    })
                    .sorted(Comparator.comparingLong(jd -> jd.creationTime))
                    .map(jd -> jd.path)
                    .collect(Collectors.toList());
        }

        for (Path jobDir : sortedJobs) {
            long usableSpace = store.getUsableSpace();
            long usedSpace = totalSpace - usableSpace;
            double currentUsedPercent = (double) usedSpace / totalSpace;

            // Stop once we are back within the safe operating range
            if (currentUsedPercent <= LOW_WATERMARK_PERCENT) {
                logger.info("Low-Watermark reached ({}%). Halting aggressive reclamation.", Math.round(currentUsedPercent * 100));
                break;
            }

            String jobId = jobDir.getFileName().toString();
            logger.info("Reclaiming Space: Spilling job directory to MinIO -> {}", jobId);

            try {
                // Spill the data to MinIO before deleting it from local disk
                if (minioStorageService != null) {
                    minioStorageService.spillJobData(jobId, jobDir);
                } else {
                    logger.warn("MinioStorageService is not initialized. Proceeding with hard deletion!");
                }

                logger.info("Spill complete. Purging local directory -> {}", jobId);
                deleteDirectoryRecursively(jobDir);
            } catch (IOException e) {
                logger.error("Failed to spill job directory {}. Skipping local deletion to prevent data loss.", jobId, e);
                // By failing safely and skipping the local deletion, we ensure data reliability
                // over disk space in the event of an object storage outage.
            }
        }
    }

    /**
     * Executes a hard TTL sweep across the shuffle directory to purge stale metadata.
     * Note: TTL sweeps skip MinIO spilling as the job data is considered fundamentally dead.
     *
     * @param rootPath The root directory containing job data.
     * @throws IOException If file attribute reading fails.
     */
    private static void purgeHardTtl(Path rootPath) throws IOException {
        long cutoff = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(HARD_TTL_HOURS);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
            for (Path jobDir : stream) {
                if (Files.isDirectory(jobDir)) {
                    BasicFileAttributes attrs = Files.readAttributes(jobDir, BasicFileAttributes.class);
                    if (attrs.creationTime().toMillis() < cutoff) {
                        logger.info("Hard TTL Expired for Job: {}. Purging data...", jobDir.getFileName());
                        deleteDirectoryRecursively(jobDir);
                    }
                }
            }
        }
    }

    /**
     * Performs a high-performance recursive deletion of a directory and all nested contents.
     *
     * @param root The directory path to delete.
     * @throws IOException If any file deletion or directory traversal operation fails.
     */
    private static void deleteDirectoryRecursively(Path root) throws IOException {
        if (!Files.exists(root)) return;

        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override @NonNull
            public FileVisitResult visitFile(@NonNull Path file, @NonNull BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override @NonNull
            public FileVisitResult postVisitDirectory(@NonNull Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}