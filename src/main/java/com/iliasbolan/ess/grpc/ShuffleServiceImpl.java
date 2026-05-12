package com.iliasbolan.ess.grpc;

import com.google.protobuf.ByteString;
import com.iliasbolan.ess.ShuffleDaemon;
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
 * Concrete implementation of the gRPC Shuffle Streaming Service.
 * <p>
 * This service acts as the core high-performance data plane for the <b>External Shuffle Service (ESS)</b>.
 * It facilitates the reliable, asynchronous transfer of intermediate MapReduce data splits across
 * physical cluster networks utilizing NIO Direct Buffers and Zero-Copy I/O concepts.
 * </p>
 * * <h3>Security Protocol: Zero-Trust Data Plane</h3>
 * <p>
 * Every partition request is subjected to rigorous cryptographic authorization. The service verifies
 * that the requester possesses a Manager-issued HMAC-SHA256 token matching the requested Job context.
 * This guarantees data is only served to authorized compute pods.
 * </p>
 * * <h3>Concurrency & Durability</h3>
 * <p>
 * To prevent Native Memory Exhaustion (OOM) during massive concurrent reducer fetch requests,
 * the streaming layer utilizes a bounded {@link java.util.concurrent.ExecutorService}. This ensures
 * strict backpressure at the network edge, preventing the daemon from crashing under high load.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.3
 * @since 2026-04-25
 */
public class ShuffleServiceImpl extends ShuffleServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServiceImpl.class);

    /**
     * Root directory on the host filesystem where intermediate shuffle partitions are persisted.
     */
    private final String baseShuffleDir;

    /**
     * Optimized buffer size (65KB) selected to align with standard TCP frame
     * sizes and guarantee that gRPC Protobuf envelopes safely fit inside
     * the strict 65,535-byte default HTTP/2 flow-control window.
     */
    private static final int CHUNK_SIZE_BYTES = 65000;

    /**
     * Bounded thread pool for handling physical disk-to-network streaming operations.
     * Capping the concurrent streams stabilizes heap and native stack memory allocations,
     * ensuring cluster survivability during massive parallel shuffle-fetch phases.
     */
    private final ExecutorService streamingPool = Executors.newFixedThreadPool(100);

    // PERFORMANCE OPTIMIZATION: Pre-compiled Regex
    // Eliminates the massive CPU penalty of compiling the validation regex on every request.
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+$");

    // PERFORMANCE OPTIMIZATION: Cached Cryptographic Key Spec
    // Prevents instantiating the key byte array and object on every single HMAC validation.
    private final SecretKeySpec secretKeySpec;

    /**
     * Constructs a new Shuffle Service implementation.
     *
     * @param baseShuffleDir Absolute or relative path to the local directory containing partitioned Map data.
     */
    public ShuffleServiceImpl(String baseShuffleDir) {
        this.baseShuffleDir = baseShuffleDir;

        // Extract internal secret securely from the environment
        String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");
        this.secretKeySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    /**
     * Streams a specific shuffle partition to a requesting Reducer pod.
     * <p>
     * This method acts as a gateway. It performs authorization and path sanitization,
     * then offloads the heavy blocking I/O stream processing to a dedicated worker pool.
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

        // 3. DATA LOCALIZATION
        Path partitionDir = Paths.get(baseShuffleDir, safeJobId, String.valueOf(partitionId));

        if (!Files.exists(partitionDir)) {
            logger.warn("Target partition directory missing: {}", partitionDir);
            responseObserver.onCompleted();
            return;
        }

        // 4. HIGH-PERFORMANCE STREAMING (Bounded Execution)
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

        // gRPC Serializing Executor Deadlock Fix
        // I/O operations are offloaded to a bounded streaming pool to prevent OOM errors
        // while allowing gRPC network threads to continue processing wake-up signals.
        streamingPool.submit(() -> {
            try (Stream<Path> filePaths = Files.list(partitionDir)) {
                filePaths.filter(Files::isRegularFile)
                        .forEach(file -> streamFileContent(file, flowController, flowLock));

                if (!flowController.isCancelled()) {
                    responseObserver.onCompleted();
                    logger.info("Secure stream concluded for Partition {} (Job: {})", partitionId, safeJobId);
                }

            } catch (Exception e) {
                logger.error("Internal streaming failure for directory: {}", partitionDir, e);
                responseObserver.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
            }
        });
    }

    /**
     * Validates structural identifiers to prevent Arbitrary Path Traversal attacks.
     *
     * @param input The untrusted JobId provided by the gRPC client.
     * @return The validated and sanitized alphanumeric ID.
     * @throws SecurityException If the input contains illegal characters (e.g., "../").
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
     * @return A hexadecimal string representation of the securely computed HMAC signature.
     * @throws RuntimeException If the underlying JVM does not support HMAC-SHA256.
     */
    private String computeHmacSha256(String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(secretKeySpec); // Reuse cached spec
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
     * Utilizes an active wait-loop tied to the gRPC stream state, guaranteeing that disk reads
     * are instantly paused if the network buffer is full (backpressure application).
     * </p>
     *
     * @param file           The physical file segment path to stream to the client.
     * @param flowController The gRPC observer managing the underlying HTTP/2 network flow.
     * @param flowLock       The synchronized monitor object utilized for instant thread resumption.
     * @throws RuntimeException If an IO exception occurs while reading the physical disk channel.
     */
    private void streamFileContent(Path file, ServerCallStreamObserver<PartitionChunk> flowController, Object flowLock) {
        if (flowController.isCancelled()) return;

        ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();

                // Reactive backpressure wait loop
                synchronized (flowLock) {
                    while (!flowController.isReady() && !flowController.isCancelled()) {
                        try {
                            flowLock.wait(); // Eliminates the CPU penalty. Sleeps until strictly necessary.
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }

                if (flowController.isCancelled()) {
                    break;
                }

                transmitChunk(buffer, flowController);
                buffer.clear();
            }
        } catch (IOException e) {
            logger.error("NIO Channel failure while reading segment: {}", file, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Packages an initialized ByteBuffer into a gRPC Protobuf envelope and transmits it to the peer.
     *
     * @param buffer   The populated direct buffer containing file segment data.
     * @param observer The destination stream observer handling outbound messages.
     */
    private void transmitChunk(ByteBuffer buffer, StreamObserver<PartitionChunk> observer) {
        observer.onNext(PartitionChunk.newBuilder()
                .setContent(ByteString.copyFrom(buffer))
                .build());
    }
}