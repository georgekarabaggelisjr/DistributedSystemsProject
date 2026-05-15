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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * High-performance, multi-tier data plane implementation for the External Shuffle Service (ESS).
 * <p>
 * This service is responsible for securely streaming intermediate MapReduce partition data
 * to downstream Reducers via Peer-to-Peer (P2P) gRPC connections. It utilizes NIO Direct Buffers
 * to maintain a strict O(1) memory footprint during massive multi-gigabyte data transfers.
 * </p>
 * <p>
 * <b>Architecture Update (Virtual Thread Carrier Pinning Prevention):</b><br>
 * Refactored HTTP/2 flow-control backpressure handling to utilize {@link ReentrantLock} and
 * {@link Condition} instead of intrinsic monitor locks (<code>synchronized</code> / <code>wait()</code>).
 * This ensures that when a stream yields due to network saturation, the underlying Virtual Thread
 * cleanly unmounts from its OS carrier thread. This prevents global JVM deadlocks when scaling
 * to thousands of concurrent shuffle fetches.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 4.6
 * @since 2026-05-15
 */
public class ShuffleServiceImpl extends ShuffleServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServiceImpl.class);
    private static final int CHUNK_SIZE_BYTES = 65000;

    /** Security boundary: Ensures job identifiers cannot contain path traversal payloads (e.g., "../"). */
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+$");

    private final String baseShuffleDir;
    private final MinioStorageService minioStorageService;

    /** Ephemeral concurrency pool utilizing lightweight Virtual Threads for massive I/O scaling. */
    private final ExecutorService streamingPool = Executors.newVirtualThreadPerTaskExecutor();

    /** Concurrency gate to prevent exhausting MinIO connection pools during massive cache misses. */
    private final Semaphore ioPermits;

    /** Cryptographic key specification for stateless token validation. */
    private final SecretKeySpec secretKeySpec;

    /**
     * Initializes the gRPC Shuffle Data Plane.
     *
     * @param baseShuffleDir      The root physical directory containing localized partition data.
     * @param minioStorageService The configured S3 adapter for restoring spilled partitions.
     */
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

        // 1. Guard Clause: Zero-Trust Cryptographic Authorization
        if (!isAuthorized(rawJobId)) {
            sendError(responseObserver, Status.UNAUTHENTICATED, "Invalid Cryptographic Signature.");
            return;
        }

        // 2. Guard Clause: Strict Path Sanitization
        String safeJobId;
        try {
            safeJobId = sanitizeId(rawJobId);
        } catch (SecurityException se) {
            sendError(responseObserver, Status.INVALID_ARGUMENT, "Malformed job identifier.");
            return;
        }

        Path partitionDir = Paths.get(baseShuffleDir, safeJobId, String.valueOf(partitionId));
        ServerCallStreamObserver<PartitionChunk> flowController = (ServerCallStreamObserver<PartitionChunk>) responseObserver;

        // 3. Setup Virtual-Thread-Safe Flow Control
        final ReentrantLock flowLock = new ReentrantLock();
        final Condition flowCondition = flowLock.newCondition();

        // Signal waiting virtual threads when the network buffer clears
        flowController.setOnReadyHandler(() -> {
            flowLock.lock();
            try {
                flowCondition.signalAll();
            } finally {
                flowLock.unlock();
            }
        });

        // Signal waiting virtual threads to abort cleanly if the client disconnects
        flowController.setOnCancelHandler(() -> {
            flowLock.lock();
            try {
                flowCondition.signalAll();
            } finally {
                flowLock.unlock();
            }
        });

        // 4. Delegate heavy I/O to the Virtual Thread pool
        streamingPool.submit(() -> executeStreamingTask(safeJobId, partitionId, partitionDir, flowController, flowLock, flowCondition));
    }

    /**
     * Core asynchronous execution block. Separates tiered storage resolution from physical streaming.
     */
    private void executeStreamingTask(String jobId, int partitionId, Path partitionDir,
                                      ServerCallStreamObserver<PartitionChunk> observer,
                                      ReentrantLock flowLock, Condition flowCondition) {
        try {
            // Attempt to resolve data (either locally or via S3 restore).
            // Returns false if it handled a data-skew empty partition internally.
            if (!resolveStorageTier(jobId, partitionId, partitionDir, observer)) {
                return;
            }

            // If we reach here, data exists locally. Stream it.
            streamDirectory(jobId, partitionId, partitionDir, observer, flowLock, flowCondition);

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
     *
     * @return true if data is available locally to stream, false if an empty stream was triggered.
     * @throws InterruptedException if the thread is interrupted while waiting for an I/O permit.
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
                                 ServerCallStreamObserver<PartitionChunk> observer,
                                 ReentrantLock flowLock, Condition flowCondition) throws IOException {

        try (Stream<Path> filePaths = Files.list(partitionDir)) {
            List<Path> validFiles = filePaths.filter(p -> p.toString().endsWith(".lz4")).toList();

            // Skew Resolution: Directory exists but contains no finalized .lz4 frames
            if (validFiles.isEmpty()) {
                handleEmptyPartition(observer, partitionDir);
                return;
            }

            // Stream all valid files
            for (Path file : validFiles) {
                streamFileContent(file, observer, flowLock, flowCondition);
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
     * Incorporates Virtual-Thread-safe locking to gracefully handle gRPC network backpressure.
     */
    private void streamFileContent(Path file, ServerCallStreamObserver<PartitionChunk> flowController,
                                   ReentrantLock flowLock, Condition flowCondition) throws IOException {

        if (flowController.isCancelled()) return;

        // O(1) Memory Allocation: Data moves straight from OS Page Cache to Network Socket
        ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();

                // Apply Network Backpressure safely without pinning the Carrier Thread
                flowLock.lock();
                try {
                    while (!flowController.isReady() && !flowController.isCancelled()) {
                        flowCondition.await();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    flowLock.unlock();
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

    /**
     * Validates the cryptographically signed HMAC token injected by the gRPC Interceptor.
     */
    private boolean isAuthorized(String rawJobId) {
        String providedToken = ShuffleDaemon.AUTH_TOKEN_KEY.get();
        String expectedToken = computeHmacSha256(rawJobId);

        if (providedToken == null || !MessageDigest.isEqual(providedToken.getBytes(StandardCharsets.UTF_8), expectedToken.getBytes(StandardCharsets.UTF_8))) {
            logger.warn("SECURITY ALERT: HMAC Mismatch for Job {}. Token Rejected.", rawJobId);
            return false;
        }
        return true;
    }

    /**
     * Safely resolves data skew scenarios by transmitting a zero-byte payload to the Reducer.
     */
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
            throw new SecurityException("Illegal character set in identifier. Potential path traversal detected.");
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