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
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Concrete implementation of the gRPC Shuffle Streaming Service.
 * <p>
 * This service is the core of the <b>External Shuffle Service (ESS)</b>, providing
 * a high-performance data plane for Peer-to-Peer shuffle fetches. By utilizing
 * NIO Direct Buffers and Zero-Copy I/O concepts, it facilitates the transfer
 * of intermediate MapReduce data splits across the physical cluster network.
 * </p>
 * <p>
 * <b>Security Protocol: Zero-Trust Data Plane.</b><br>
 * Every request is subjected to cryptographic authorization. The service verifies
 * that the requester possesses a Manager-issued HMAC-SHA256 token, ensuring that
 * data is only served to authorized compute pods belonging to the same Job context.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.2
 * @since 2026-04-25
 */
public class ShuffleServiceImpl extends ShuffleServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleServiceImpl.class);

    /**
     * Root directory on the host filesystem where intermediate shuffle partitions are persisted.
     */
    private final String baseShuffleDir;

    /**
     * Optimized buffer size (64KB) selected to align with standard TCP frame
     * sizes and minimize context switching during NIO operations.
     */
    private static final int CHUNK_SIZE_BYTES = 65536;

    // PERFORMANCE OPTIMIZATION: Pre-compiled Regex
    // Eliminates the massive CPU penalty of compiling the validation regex on every request.
    private static final Pattern ID_PATTERN = Pattern.compile("^[a-zA-Z0-9\\-]+$");

    // PERFORMANCE OPTIMIZATION: Cached Cryptographic Key Spec
    // Prevents instantiating the key byte array and object on every single HMAC validation.
    private final SecretKeySpec secretKeySpec;

    /**
     * Constructs a new Shuffle Service implementation.
     *
     * @param baseShuffleDir Path to the local directory containing partitioned Map data.
     */
    public ShuffleServiceImpl(String baseShuffleDir) {
        this.baseShuffleDir = baseShuffleDir;

        String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");
        this.secretKeySpec = new SecretKeySpec(secretKey.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
    }

    /**
     * Streams a specific shuffle partition to a requesting Reducer pod.
     *
     * @param request          Contains the JobId and PartitionId to be fetched.
     * @param responseObserver Stream observer for transmitting asynchronous {@link PartitionChunk} sequences.
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

        // 4. HIGH-PERFORMANCE STREAMING
        ServerCallStreamObserver<PartitionChunk> flowController =
                (ServerCallStreamObserver<PartitionChunk>) responseObserver;

        // PERFORMANCE OPTIMIZATION: Reactive Flow Control Monitor
        // Defines a synchronized monitor that allows the gRPC network stack to
        // instantly wake up the disk-reader thread when buffer capacity is available.
        final Object flowLock = new Object();
        flowController.setOnReadyHandler(() -> {
            synchronized (flowLock) {
                flowLock.notifyAll();
            }
        });

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
    }

    /**
     * Validates structural identifiers to prevent Arbitrary Path Traversal attacks.
     *
     * @param input The untrusted JobId provided by the gRPC client.
     * @return The validated and sanitized ID.
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
     * @param data The Job ID to sign.
     * @return A hexadecimal string representation of the HMAC signature.
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
     * Streams the content of a physical file using NIO Direct Buffers.
     *
     * @param file           The physical file segment to stream.
     * @param flowController The gRPC observer controlling the network flow.
     * @param flowLock       The synchronized monitor for instant thread resumption.
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
                            flowLock.wait(); // Eliminates the 5ms penalty. Sleeps until strictly necessary.
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
     * Packages a ByteBuffer into a gRPC message and transmits it to the peer.
     *
     * @param buffer   The direct buffer containing file segment data.
     * @param observer The destination stream observer.
     */
    private void transmitChunk(ByteBuffer buffer, StreamObserver<PartitionChunk> observer) {
        observer.onNext(PartitionChunk.newBuilder()
                .setContent(ByteString.copyFrom(buffer))
                .build());
    }
}