package com.iliasbolan.engine.shuffle;

import com.google.protobuf.ByteString;
import com.iliasbolan.grpc.shuffle.PartitionChunk;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc.ShuffleServiceImplBase;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Hosts the Peer-to-Peer (P2P) gRPC Server for the MapReduce Shuffle phase.
 * <p>
 * This server provides the high-performance data plane for the distributed cluster.
 * During the Map phase, intermediate data is persisted locally by the {@link ShufflePartitioner}.
 * This class exposes that data via a gRPC interface, allowing sibling Reduce nodes to stream
 * required partitions directly from the source worker without routing through centralized storage.
 * </p>
 * <h3>Architectural Design:</h3>
 * <ul>
 * <li><b>Memory-Safe Streaming:</b> Implements gRPC Server-Side Streaming to handle large-scale
 * datasets. It buffers file content into sequential 64KB chunks, ensuring a constant and
 * low memory footprint regardless of total partition size.</li>
 * <li><b>Kubernetes Lifecycle Integration:</b> Includes standard shutdown hooks to ensure
 * active network streams are completed before a pod is reclaimed by the cluster orchestrator.</li>
 * <li><b>Non-blocking I/O:</b> Leverages the Netty-based gRPC transport layer for low-latency
 * worker-to-worker communication.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-23
 * @see ShufflePartitioner
 * @see com.iliasbolan.engine.TaskExecutor
 */
public class ShuffleGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(ShuffleGrpcServer.class);

    private final int port;
    private final Server server;

    /**
     * Constructs a new Shuffle gRPC Server with the specified network and storage configuration.
     *
     * @param port           The network port the server will bind to (e.g., 50051).
     * @param baseShuffleDir The root directory on the local disk where intermediate partition
     * files are stored.
     */
    public ShuffleGrpcServer(int port, String baseShuffleDir) {
        this.port = port;
        this.server = ServerBuilder.forPort(port)
                // Register the core RPC handler implementation
                .addService(new ShuffleServiceImpl(baseShuffleDir))
                .build();
    }

    /**
     * Initializes and starts the gRPC server.
     * <p>
     * Once started, the server begins listening for incoming {@code GetPartition} requests
     * from Reducer nodes. A JVM shutdown hook is automatically registered to manage
     * resource cleanup.
     * </p>
     *
     * @throws IOException If the network port is unavailable or the server cannot bind to
     * the interface.
     */
    public void start() throws IOException {
        server.start();
        logger.info("gRPC Shuffle Server successfully started. Listening for P2P connections on port {}", port);

        // Ensure active streams are given time to flush during pod termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn("JVM shutting down. Initiating graceful shutdown of gRPC Shuffle Server...");
            try {
                ShuffleGrpcServer.this.stop();
            } catch (InterruptedException e) {
                logger.error("Interrupted during gRPC server shutdown.", e);
            }
        }));
    }

    /**
     * Gracefully terminates the server and releases all associated resources.
     * <p>
     * The method waits up to 5 seconds for active RPC calls to complete before
     * forcefully dropping any remaining connections.
     * </p>
     *
     * @throws InterruptedException If the current thread is interrupted while awaiting
     * termination.
     */
    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            logger.info("gRPC Shuffle Server stopped.");
        }
    }

    /**
     * Blocks the calling thread until the server has been shut down.
     * <p>
     * This is useful in standalone execution modes where the main thread needs
     * to keep the JVM alive while the gRPC daemon threads handle requests.
     * </p>
     *
     * @throws InterruptedException If the blocking thread is interrupted.
     */
    @SuppressWarnings("unused")
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Concrete implementation of the Protobuf {@code ShuffleService} interface.
     * <p>
     * Handles incoming requests by mapping partition identifiers to local file system
     * paths and streaming the contents to clients.
     * </p>
     */
    private static class ShuffleServiceImpl extends ShuffleServiceImplBase {

        private final String baseShuffleDir;

        /**
         * @param baseShuffleDir The root directory on disk where Map worker output is organized.
         */
        public ShuffleServiceImpl(String baseShuffleDir) {
            this.baseShuffleDir = baseShuffleDir;
        }

        /**
         * Orchestrates the server-side streaming of partition data.
         * <p>
         * It locates the specific directory for the requested Job and Partition, iterates
         * through all local Map output files, and pipes their contents into the response stream.
         * </p>
         *
         * @param request          The RPC request containing the Job ID and Partition ID.
         * @param responseObserver The observer used to push data chunks and signal completion
         * to the client.
         */
        @Override
        public void getPartition(PartitionRequest request, StreamObserver<PartitionChunk> responseObserver) {
            String jobId = request.getJobId();
            int partitionId = request.getPartitionId();

            logger.info("Received P2P streaming request for Job: {}, Partition: {}", jobId, partitionId);

            Path partitionDir = Paths.get(baseShuffleDir, jobId, String.valueOf(partitionId));

            // Basic validation to prevent unnecessary stream overhead
            if (!Files.exists(partitionDir) || !Files.isDirectory(partitionDir)) {
                logger.warn("Requested partition directory does not exist: {}. Yielding empty stream.", partitionDir);
                responseObserver.onCompleted();
                return;
            }

            try (Stream<Path> filePaths = Files.list(partitionDir)) {
                // Process each individual map-result file within the partition directory
                filePaths.filter(Files::isRegularFile).forEach(file -> streamFileContent(file, responseObserver));

                // Signal the end of the P2P transfer
                responseObserver.onCompleted();
                logger.info("Successfully completed streaming Partition {} for Job {}.", partitionId, jobId);

            } catch (IOException e) {
                logger.error("File system error while reading partition directory: {}", partitionDir, e);
                responseObserver.onError(io.grpc.Status.INTERNAL
                        .withDescription("Failed to read partition files on source node.")
                        .withCause(e)
                        .asRuntimeException());
            }
        }

        /**
         * Efficiently streams a local file to the gRPC response observer.
         * <p>
         * This method implements the 64KB buffering strategy to maintain memory safety.
         * </p>
         *
         * @param file             The path to the file on the local disk.
         * @param responseObserver The stream used to transmit data to the Reducer.
         */
        private void streamFileContent(Path file, StreamObserver<PartitionChunk> responseObserver) {
            // Enforce UTF-8 encoding
            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                StringBuilder chunkBuilder = new StringBuilder();

                lines.forEach(line -> {
                    chunkBuilder.append(line).append("\n");

                    // 64KB Threshold check to optimize network MTU and JVM Heap usage
                    if (chunkBuilder.length() > 65536) {
                        PartitionChunk chunk = PartitionChunk.newBuilder()
                                .setContent(ByteString.copyFromUtf8(chunkBuilder.toString()))
                                .build();
                        responseObserver.onNext(chunk);

                        // Reset internal buffer for next segment
                        chunkBuilder.setLength(0);
                    }
                });

                // Final flush for data remaining in the buffer
                if (!chunkBuilder.isEmpty()) {
                    PartitionChunk chunk = PartitionChunk.newBuilder()
                            .setContent(ByteString.copyFromUtf8(chunkBuilder.toString()))
                            .build();
                    responseObserver.onNext(chunk);
                }

            } catch (IOException e) {
                logger.error("Error reading specific partition file: {}", file.toAbsolutePath(), e);
                throw new RuntimeException("Failed to stream specific file.", e);
            }
        }
    }
}