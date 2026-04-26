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
import java.util.concurrent.TimeUnit;
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
 * @version 1.1
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

    /**
     * Pre-shared key used for HMAC signature verification.
     * Loaded once at instantiation to minimize environment overhead during high-frequency streaming.
     */
    private final String secretKey = System.getenv().getOrDefault("ESS_SECRET_KEY", "dev-insecure-shared-secret");

    /**
     * Constructs a new Shuffle Service implementation.
     *
     * @param baseShuffleDir Path to the local directory containing partitioned Map data.
     */
    public ShuffleServiceImpl(String baseShuffleDir) {
        this.baseShuffleDir = baseShuffleDir;
    }

    /**
     * Streams a specific shuffle partition to a requesting Reducer pod.
     * <p>
     * Implementation Details:
     * <ol>
     * <li>Performs constant-time HMAC verification of the Job Authorization Token.</li>
     * <li>Sanitizes inputs to prevent Path Traversal attacks.</li>
     * <li>Localizes the physical partition directory based on Job and Partition IDs.</li>
     * <li>Initiates a sequential NIO stream of all segment files within the partition.</li>
     * </ol>
     * </p>
     *
     * @param request          Contains the JobId and PartitionId to be fetched.
     * @param responseObserver Stream observer for transmitting asynchronous {@link PartitionChunk} sequences.
     */
    @Override
    public void getPartition(PartitionRequest request, StreamObserver<PartitionChunk> responseObserver) {
        String rawJobId = request.getJobId();
        int partitionId = request.getPartitionId();

        logger.info("ESS Stream Request: [Job: {}, Partition: {}]", rawJobId, partitionId);

        // --- 1. ZERO-TRUST AUTHORIZATION GATEWAY ---
        String providedToken = ShuffleDaemon.AUTH_TOKEN_KEY.get();
        String expectedToken = computeHmacSha256(rawJobId, secretKey);

        if (providedToken == null || !MessageDigest.isEqual(providedToken.getBytes(), expectedToken.getBytes())) {
            logger.warn("SECURITY BREACH ATTEMPT: Unauthorized access to Job {}. Token Rejected.", rawJobId);
            responseObserver.onError(io.grpc.Status.UNAUTHENTICATED
                    .withDescription("Missing or Invalid Cryptographic Authorization Token.")
                    .asRuntimeException());
            return;
        }

        // --- 2. INPUT SANITIZATION ---
        // Prevent Arbitrary Path Traversal by Reducer pods
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

        // --- 3. DATA LOCALIZATION ---
        Path partitionDir = Paths.get(baseShuffleDir, safeJobId, String.valueOf(partitionId));

        if (!Files.exists(partitionDir)) {
            logger.warn("Target partition directory missing: {}", partitionDir);
            responseObserver.onCompleted();
            return;
        }

        // --- 4. HIGH-PERFORMANCE STREAMING ---
        try (Stream<Path> filePaths = Files.list(partitionDir)) {
            filePaths.filter(Files::isRegularFile)
                    .forEach(file -> streamFileContent(file, responseObserver));

            responseObserver.onCompleted();
            logger.info("Secure stream concluded for Partition {} (Job: {})", partitionId, safeJobId);

        } catch (Exception e) {
            logger.error("Internal streaming failure for directory: {}", partitionDir, e);
            responseObserver.onError(io.grpc.Status.INTERNAL.withCause(e).asRuntimeException());
        }
    }

    /**
     * Validates structural identifiers to prevent Arbitrary Path Traversal attacks.
     * Ensures IDs only contain alphanumeric characters and standard delimiters.
     *
     * @param input The untrusted JobId provided by the gRPC client.
     * @return The validated and sanitized ID.
     * @throws SecurityException If malicious path-traversal characters are detected.
     */
    private String sanitizeId(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9\\-]+$")) {
            throw new SecurityException("Invalid ID format. Potential path traversal detected.");
        }
        return input;
    }

    /**
     * Computes an HMAC-SHA256 signature to validate the request origin.
     *
     * @param data The Job ID to sign.
     * @param key  The pre-shared secret key.
     * @return A hexadecimal string representation of the HMAC signature.
     */
    private String computeHmacSha256(String data, String key) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKeySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
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
     * Streams the content of a physical file using NIO Direct Buffers.
     *
     * @param file             The physical file segment to stream.
     * @param responseObserver The gRPC observer for chunk transmission.
     */
    private void streamFileContent(Path file, StreamObserver<PartitionChunk> responseObserver) {
        ServerCallStreamObserver<PartitionChunk> flowController =
                (ServerCallStreamObserver<PartitionChunk>) responseObserver;

        ByteBuffer buffer = ByteBuffer.allocateDirect(CHUNK_SIZE_BYTES);

        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            while (fileChannel.read(buffer) > 0) {
                buffer.flip();

                waitForFlowControl(flowController);
                transmitChunk(buffer, flowController);

                buffer.clear();
            }
        } catch (IOException e) {
            logger.error("NIO Channel failure while reading segment: {}", file, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Implements a spin-lock wait to respect gRPC flow control signals.
     *
     * @param observer The observer providing the <code>isReady</code> status.
     */
    private void waitForFlowControl(ServerCallStreamObserver<PartitionChunk> observer) {
        while (!observer.isReady() && !observer.isCancelled()) {
            try {
                TimeUnit.MILLISECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
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