package com.iliasbolan.ess.grpc;

import com.google.protobuf.ByteString;
import com.iliasbolan.ess.ShuffleDaemon;
import com.iliasbolan.ess.storage.MinioStorageService;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc.ShuffleServiceImplBase;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * High-performance, multi-tier data plane implementation for the External Shuffle Service (ESS).
 * <p>
 * Refactored for improved readability, utilizing guard clauses and method extraction
 * to flatten the "Arrow Anti-Pattern" of nested concurrency blocks.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 4.5 (Refactored)
 * @since 2026-05-15
 */
public class ShuffleServiceImpl extends ShuffleServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServiceImpl.class);
    private static final int CHUNK_SIZE_BYTES = 65000;
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+$");

    private final String baseShuffleDir;
    private final MinioStorageService minioStorageService;
    private final ExecutorService streamingPool = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore ioPermits;
    private final SecretKeySpec secretKeySpec;

    public ShuffleServiceImpl(String baseShuffleDir, MinioStorageService minioStorageService) {
        this.baseShuffleDir = baseShuffleDir;
        this.minioStorageService = minioStorageService;

        String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");
        this.secretKeySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");

        int maxConcurrentIo = Integer.parseInt(System.getenv().getOrDefault("ESS_MAX_CONCURRENT_IO", "150"));
        this.ioPermits = new Semaphore(maxConcurrentIo, true);
        logger.info("ShuffleService Data Plane initialized. S3 Restore Ceiling: {}.", maxConcurrentIo);
    }

    @Override
    public void getPartition(PartitionRequest request, StreamObserver<PartitionChunk> responseObserver) {
        String rawJobId = request.getJobId();
        int partitionId = request.getPartitionId();

        logger.info("Inbound Stream Request: [Job: {}, Partition: {}]", rawJobId, partitionId);

        // 1. Guard Clause: Authorization
        if (!isAuthorized(rawJobId)) {
            sendError(responseObserver, Status.UNAUTHENTICATED, "Invalid Cryptographic Signature.");
            return;
        }

        // 2. Guard Clause: Path Sanitization
        String safeJobId;
        try {
            safeJobId = sanitizeId(rawJobId);
        } catch (SecurityException se) {
            sendError(responseObserver, Status.INVALID_ARGUMENT, "Malformed job identifier.");
            return;
        }

        // 3. Setup Flow Control & Delegate to Virtual Thread
        Path partitionDir = Paths.get(baseShuffleDir, safeJobId, String.valueOf(partitionId));
        ServerCallStreamObserver<PartitionChunk> flowController = (ServerCallStreamObserver<PartitionChunk>) responseObserver;

        final Object flowLock = new Object();
        flowController.setOnReadyHandler(() -> { synchronized (flowLock) { flowLock.notifyAll(); } });
        flowController.setOnCancelHandler(() -> { synchronized (flowLock) { flowLock.notifyAll(); } });

        streamingPool.submit(() -> executeStreamingTask(safeJobId, partitionId, partitionDir, flowController, flowLock));
    }

    /**
     * Core asynchronous execution block. Separates tiered storage resolution from physical streaming.
     */
    private void executeStreamingTask(String jobId, int partitionId, Path partitionDir,
                                      ServerCallStreamObserver<PartitionChunk> observer, Object flowLock) {
        try {
            // Attempt to resolve data (either locally or via S3 restore).
            // Returns false if it handled a data-skew empty partition internally.
            if (!resolveStorageTier(jobId, partitionId, partitionDir, observer)) {
                return;
            }

            // If we reach here, data exists locally. Stream it.
            streamDirectory(jobId, partitionId, partitionDir, observer, flowLock);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendError(observer, Status.CANCELLED, "I/O Interrupted before execution.");
        } catch (Exception e) {
            logger.error("Data Plane Error for directory: {}", partitionDir, e);
            sendError(observer, Status.INTERNAL, e.getMessage());
        }
    }

    /**
     * Resolves cache misses by pulling from MinIO. Handles semaphore gating cleanly.
     * @return true if data is available locally to stream, false if an empty stream was triggered.
     */
    private boolean resolveStorageTier(String jobId, int partitionId, Path partitionDir, StreamObserver<PartitionChunk> observer) throws InterruptedException {
        // Fast path: Data is already local
        if (Files.exists(partitionDir)) {
            return true;
        }

        // Slow path: Cache miss. Acquire permit and attempt MinIO restore.
        ioPermits.acquire();
        try {
            // Double-check locking: Ensure another thread didn't just restore it
            if (Files.exists(partitionDir)) {
                return true;
            }

            if (minioStorageService == null) {
                handleEmptyPartition(observer, partitionDir);
                return false;
            }

            logger.warn("Cache Miss for Partition {}. Restoring from S3...", partitionDir);
            minioStorageService.restorePartition(jobId, partitionId, partitionDir);
            return true;

        } catch (Exception e) {
            // Skew Resolution: Restore failed, likely because the partition legitimately has no data
            handleEmptyPartition(observer, partitionDir);
            return false;
        } finally {
            ioPermits.release();
        }
    }

    /**
     * Locates LZ4 files in the directory and streams them.
     */
    private void streamDirectory(String jobId, int partitionId, Path partitionDir,
                                 ServerCallStreamObserver<PartitionChunk> observer, Object flowLock) throws IOException {

        try (Stream<Path> filePaths = Files.list(partitionDir)) {
            List<Path> validFiles = filePaths.filter(p -> p.toString().endsWith(".lz4")).toList();

            // Skew Resolution: Directory exists but contains no finalized .lz4 frames
            if (validFiles.isEmpty()) {
                handleEmptyPartition(observer, partitionDir);
                return;
            }

            // Stream all valid files
            for (Path file : validFiles) {
                streamFileContent(file, observer, flowLock);
                if (observer.isCancelled()) break;
            }

            if (!observer.isCancelled()) {
                observer.onCompleted();
                logger.info("Stream Concluded: Partition {} (Job: {})", partitionId, jobId);
            }
        }
    }

    /**
     * Streams the physical content of a single file segment using NIO Direct Buffers.
     */
    private void streamFileContent(Path file, ServerCallStreamObserver<PartitionChunk> flowController, Object flowLock) throws IOException {
        if (flowController.isCancelled()) return;

        ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();

                // Apply Network Backpressure
                synchronized (flowLock) {
                    while (!flowController.isReady() && !flowController.isCancelled()) {
                        try {
                            flowLock.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }

                if (flowController.isCancelled()) break;

                transmitChunk(buffer, flowController);
                buffer.clear();
            }
        }
    }

    // ==========================================
    // Utility & Security Methods Below
    // ==========================================

    private boolean isAuthorized(String rawJobId) {
        String providedToken = ShuffleDaemon.AUTH_TOKEN_KEY.get();
        String expectedToken = computeHmacSha256(rawJobId);

        if (providedToken == null || !MessageDigest.isEqual(providedToken.getBytes(StandardCharsets.UTF_8), expectedToken.getBytes(StandardCharsets.UTF_8))) {
            logger.warn("SECURITY ALERT: HMAC Mismatch for Job {}. Token Rejected.", rawJobId);
            return false;
        }
        return true;
    }

    private void handleEmptyPartition(StreamObserver<PartitionChunk> observer, Path dir) {
        logger.info("Data Skew Resolution: Injecting empty chunk for partition {}", dir);
        observer.onNext(PartitionChunk.newBuilder().setContent(ByteString.EMPTY).build());
        observer.onCompleted();
    }

    private void sendError(StreamObserver<PartitionChunk> observer, Status status, String description) {
        observer.onError(status.withDescription(description).asRuntimeException());
    }

    private String sanitizeId(String input) {
        if (input == null || !ID_PATTERN.matcher(input).matches()) {
            throw new SecurityException("Illegal character set in identifier.");
        }
        return input;
    }

    private String computeHmacSha256(String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(secretKeySpec);
            byte[] hashBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            throw new RuntimeException("Cryptographic provider initialization failed.", e);
        }
    }

    private void transmitChunk(ByteBuffer buffer, StreamObserver<PartitionChunk> observer) {
        observer.onNext(PartitionChunk.newBuilder().setContent(ByteString.copyFrom(buffer)).build());
    }
}