package com.iliasbolan.ess.grpc;

import com.google.protobuf.ByteString;
import com.iliasbolan.ess.ShuffleDaemon;
import com.iliasbolan.ess.storage.MinioStorageService;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc.ShuffleServiceImplBase;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Concrete implementation of the gRPC Shuffle Streaming Service with Multi-Tier Storage Support.
 * <p>
 * This service acts as the core high-performance data plane for the <b>External Shuffle Service (ESS)</b>.
 * It facilitates the reliable, asynchronous transfer of intermediate MapReduce data splits across
 * physical cluster networks utilizing NIO Direct Buffers and Zero-Copy I/O concepts.
 * </p>
 * <p>
 * <b>LZ4 Pass-Through Optimization:</b><br>
 * This service acts as a transparent binary pipeline, streaming {@code .lz4} blocks from local disk
 * directly to the gRPC wire. By bypassing decompression, we preserve host CPU resources and ensure format integrity.
 * </p>
 * <p>
 * <b>Resilient Data Plane (MinIO Overflow):</b><br>
 * Integrated with an S3-compatible overflow tier. If an inbound request targets a partition that
 * has been evicted from the local physical disk by the Janitor, this service will automatically perform
 * a Lazy-Restore, downloading the missing data from MinIO before fulfilling the stream. This prevents
 * job failures for lagging straggler Reducers.
 * </p>
 * <p>
 * <b>Security Protocol: Zero-Trust Data Plane</b><br>
 * Every partition request is subjected to rigorous cryptographic authorization. The service verifies
 * that the requester possesses a Manager-issued HMAC-SHA256 token matching the requested Job context.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-05-13
 */
public class ShuffleServiceImpl extends ShuffleServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServiceImpl.class);

    /** Root directory on the host filesystem where intermediate shuffle partitions are persisted. */
    private final String baseShuffleDir;

    /** Cloud-native overflow storage client for retrieving evicted partition data. */
    private final MinioStorageService minioStorageService;

    /**
     * Optimized buffer size (65KB) selected to align with standard TCP frame
     * sizes and guarantee that gRPC Protobuf envelopes safely fit inside
     * the strict 65,535-byte default HTTP/2 flow-control window.
     */
    private static final int CHUNK_SIZE_BYTES = 65000;

    /**
     * Bounded thread pool for handling physical disk-to-network streaming and S3 restoration.
     */
    private final ExecutorService streamingPool = Executors.newFixedThreadPool(100);

    /** Pre-compiled regex for validating Job and Partition identifiers. */
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+$");

    /** Cached cryptographic key specification for HMAC validation. */
    private final SecretKeySpec secretKeySpec;

    /**
     * Constructs a new Shuffle Service implementation with overflow tier integration.
     *
     * @param baseShuffleDir      Absolute or relative path to the local directory containing partitioned Map data.
     * @param minioStorageService S3 adapter for restoring data evicted by the high-watermark Janitor.
     */
    public ShuffleServiceImpl(String baseShuffleDir, MinioStorageService minioStorageService) {
        this.baseShuffleDir = baseShuffleDir;
        this.minioStorageService = minioStorageService;

        // Extract internal secret securely from the environment
        String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");
        this.secretKeySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    /**
     * Streams a specific shuffle partition to a requesting Reducer pod.
     * <p>
     * Performs authorization and path sanitization, then offloads the heavy binary
     * streaming and potential MinIO object restoration to a dedicated worker pool.
     * </p>
     *
     * @param request          The Protobuf message containing the requested JobId and PartitionId.
     * @param responseObserver The gRPC Stream observer for transmitting asynchronous {@link PartitionChunk} sequences.
     */
    @Override
    public void getPartition(PartitionRequest request, StreamObserver<PartitionChunk> responseObserver) {
        String rawJobId = request.getJobId();
        int partitionId = request.getPartitionId();

        logger.info("ESS Stream Request: [Job: {}, Partition: {}]", rawJobId, partitionId);

        // 1. ZERO-TRUST AUTHORIZATION GATEWAY
        String providedToken = ShuffleDaemon.AUTH_TOKEN_KEY.get();
        String expectedToken = computeHmacSha256(rawJobId);

        if (providedToken == null || !MessageDigest.isEqual(providedToken.getBytes(StandardCharsets.UTF_8), expectedToken.getBytes(StandardCharsets.UTF_8))) {
            logger.warn("SECURITY BREACH ATTEMPT: Unauthorized access to Job {}. Token Rejected.", rawJobId);
            responseObserver.onError(io.grpc.Status.UNAUTHENTICATED
                    .withDescription("Missing or Invalid Cryptographic Authorization Token.")
                    .asRuntimeException());
            return;
        }

        // 2. INPUT SANITIZATION
        String safeJobId;
        try {
            safeJobId = sanitizeId(rawJobId);
        } catch (SecurityException se) {
            logger.error("SECURITY BREACH ATTEMPT: Path traversal payload detected in JobId: {}", rawJobId);
            responseObserver.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("Malformed JobId structure.")
                    .asRuntimeException());
            return;
        }

        Path partitionDir = Paths.get(baseShuffleDir, safeJobId, String.valueOf(partitionId));

        // 3. HIGH-PERFORMANCE STREAMING & ASYNC RESTORATION (Bounded Execution)
        ServerCallStreamObserver<PartitionChunk> flowController =
                (ServerCallStreamObserver<PartitionChunk>) responseObserver;

        final Object flowLock = new Object();
        flowController.setOnReadyHandler(() -> {
            synchronized (flowLock) {
                flowLock.notifyAll();
            }
        });

        flowController.setOnCancelHandler(() -> {
            synchronized (flowLock) {
                flowLock.notifyAll();
            }
        });

        // Disk I/O and S3 Networking operations are offloaded to a bounded streaming pool.
        // This prevents massive blockages on the core gRPC Netty event loops.
        streamingPool.submit(() -> {
            try {
                // TIERING LOGIC: If missing locally, block this worker thread to pull from S3
                if (!Files.exists(partitionDir)) {
                    logger.warn("Local cache miss for partition {}. Initiating MinIO overflow restore...", partitionDir);
                    if (minioStorageService != null) {
                        minioStorageService.restorePartition(safeJobId, partitionId, partitionDir);
                    } else {
                        throw new IOException("Data missing locally and MinIO overflow tier is not initialized.");
                    }
                }

                // STREAMING LOGIC: Stream LZ4 files from local physical disk
                try (Stream<Path> filePaths = Files.list(partitionDir)) {
                    filePaths.filter(p -> p.toString().endsWith(".lz4"))
                            .forEach(file -> streamFileContent(file, flowController, flowLock));

                    if (!flowController.isCancelled()) {
                        responseObserver.onCompleted();
                        logger.info("Secure LZ4 stream concluded for Partition {} (Job: {})", partitionId, safeJobId);
                    }
                }

            } catch (Exception e) {
                logger.error("Internal streaming or restore failure for directory: {}", partitionDir, e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
            }
        });
    }

    /**
     * Validates structural identifiers to prevent Arbitrary Path Traversal attacks.
     *
     * @param input The untrusted JobId provided by the gRPC client.
     * @return The validated and sanitized alphanumeric ID.
     */
    private String sanitizeId(String input) {
        if (input == null || !ID_PATTERN.matcher(input).matches()) {
            throw new SecurityException("Invalid ID format. Potential path traversal detected.");
        }
        return input;
    }

    /**
     * Computes an HMAC-SHA256 signature to validate the request origin.
     *
     * @param data The Job ID to cryptographically sign.
     * @return A hexadecimal string representation of the HMAC signature.
     */
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
            throw new RuntimeException("Failed to initialize cryptographic HMAC validation", e);
        }
    }

    /**
     * Streams the physical content of a single file segment using NIO Direct Buffers.
     * <p>
     * Utilizes an active wait-loop tied to the gRPC stream state to apply backpressure.
     * In this LZ4-only architecture, it performs a pure binary copy from disk to network.
     * </p>
     *
     * @param file           The physical .lz4 file segment path.
     * @param flowController The gRPC observer managing network flow.
     * @param flowLock       The monitor object used for thread resumption.
     */
    private void streamFileContent(Path file, ServerCallStreamObserver<PartitionChunk> flowController, Object flowLock) {
        if (flowController.isCancelled()) return;

        // Use direct buffer to minimize heap impact during massive transfers
        ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();

                synchronized (flowLock) {
                    while (!flowController.isReady() && !flowController.isCancelled()) {
                        try {
                            flowLock.wait(); // Pause reading until network buffer drains
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
        } catch (IOException e) {
            logger.error("NIO Channel failure while reading segment: {}", file, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Packages a direct buffer into a gRPC Protobuf envelope and transmits it to the peer.
     */
    private void transmitChunk(ByteBuffer buffer, StreamObserver<PartitionChunk> observer) {
        observer.onNext(PartitionChunk.newBuilder()
                .setContent(ByteString.copyFrom(buffer))
                .build());
    }
}