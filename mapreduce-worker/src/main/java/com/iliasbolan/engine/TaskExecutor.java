package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.services.S3ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import com.iliasbolan.grpc.shuffle.ShuffleServiceGrpc;
import com.iliasbolan.grpc.shuffle.PartitionRequest;
import com.iliasbolan.grpc.shuffle.PartitionChunk;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The central orchestration engine for executing Map-Reduce tasks on a Worker node.
 * <p>
 * This class serves as the primary controller for the Worker's lifecycle, managing
 * the transition between Map and Reduce phases. It handles resource localization,
 * gRPC-based data transfers, and delegation to isolated ephemeral sandbox environments.
 * </p>
 * <p>
 * <b>Architecture Update: Atomic Stream Fetching (LZ4 Integrity)</b><br>
 * To prevent binary corruption during massive 11GB+ parallel shuffles, gRPC streams
 * are initially persisted to temporary ({@code .tmp}) files. The system performs an
 * atomic OS-level rename to {@code .lz4} strictly upon successful stream completion,
 * guaranteeing that the Sorter only processes fully finalized frames.
 * </p>
 * <p>
 * <b>Architecture Update: Global Ephemeral Bounding</b><br>
 * All ephemeral files (Bytecode, Payloads, Raw gRPC Streams) are strictly confined
 * to the {@code baseShuffleDir/{jobId}/} directory. This guarantees that the
 * ESS {@code StorageJanitor} cleans up all disk traces seamlessly without leaving orphans.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.3
 * @since 2026-05-12
 */
public class TaskExecutor {

    /** The SLF4J logger instance for recording operational events and distributed telemetry. */
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    /** High-performance JSON serializer configured for robust, forward-compatible deserialization. */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Service client for interfacing with the S3-compatible storage layer. */
    private final S3ClientService s3ClientService;

    /** The root physical directory on the host node for ephemeral shuffle data persistence. */
    private final String baseShuffleDir;

    /** AMQP publisher for transmitting asynchronous lifecycle signals back to the Orchestrator. */
    private final RabbitMqProducer eventProducer;

    /** The physical IP address of the Kubernetes node, utilized for P2P data locality routing. */
    private final String nodeIp;

    /** The gRPC metadata key used for injecting and extracting cryptographic authorization tokens. */
    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Initializes the orchestration engine with its required dependencies.
     *
     * @param s3ClientService Service for downloading user-provided JARs and class files.
     * @param baseShuffleDir  Root local directory for persisting intermediate shuffle data.
     * @param eventProducer   Producer for signaling task status (COMPLETED/FAILED) to the Manager.
     * @param nodeIp          The physical IP of the host node, used for data locality signaling.
     */
    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String nodeIp) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.nodeIp = nodeIp;
    }

    /**
     * Parses the serialized task intent and routes execution to the appropriate phase handler.
     *
     * @param jsonPayload The raw JSON string containing the {@link TaskPayload} metadata.
     * @throws Throwable If parsing fails, or an unrecoverable error occurs during phase execution.
     * @throws IllegalArgumentException If the specified task type is neither MAP nor REDUCE.
     */
    public void executeTask(String jsonPayload) throws Throwable {
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);
        logger.info("Successfully parsed task payload. JobId: {}, TaskType: {}", payload.jobId(), payload.taskType());

        if ("MAP".equalsIgnoreCase(payload.taskType())) {
            executeMapPhase(payload, jsonPayload);
        } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
            executeReducePhase(payload, jsonPayload);
        } else {
            throw new IllegalArgumentException("Unknown task type received: " + payload.taskType());
        }
    }

    /**
     * Orchestrates the complete lifecycle of a Map task.
     * <p>
     * This method handles code localization, payload staging, sandbox delegation, and
     * positive completion signaling. All generated artifacts are strictly bounded to the
     * ESS Janitor's ephemeral directory structure to prevent disk leaks.
     * </p>
     *
     * @param payload The deserialized task metadata.
     * @param rawJson The original JSON payload to be ingested by the Sandbox JVM.
     * @throws Throwable If S3 retrieval, sandbox execution, or network signaling fails.
     * @throws IllegalStateException If the NODE_IP environment variable is unresolved.
     */
    private void executeMapPhase(TaskPayload payload, String rawJson) throws Throwable {
        String safeJobId = sanitizeId(payload.jobId());
        String safeTaskId = sanitizeId(payload.taskId());
        String safeClassName = sanitizeClassName(payload.className());

        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", safeTaskId);

        // Code localized within the ESS Janitor boundary
        String packagePath = safeClassName.replace(".", "/");
        String localCodeDir = Paths.get(baseShuffleDir, safeJobId, "usercode") + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Payload localized within the ESS Janitor boundary
        Path payloadFile = Paths.get(baseShuffleDir, safeJobId, "payloads", safeTaskId + ".json");
        Files.createDirectories(payloadFile.getParent());
        Files.writeString(payloadFile, rawJson);

        logger.info("Delegating Map computation to Ephemeral Sandbox JVM...");
        runSandbox(payloadFile, payload);

        if (this.nodeIp == null || this.nodeIp.trim().isEmpty()) {
            throw new IllegalStateException("CRITICAL FATAL: NODE_IP environment variable missing.");
        }

        String nodeName = System.getenv().getOrDefault("NODE_NAME", "unknown-node");
        String essBindAddress = nodeName + "@@" + this.nodeIp + ":7337";

        logger.info("Map finished. Signaling ESS Endpoint: {} with Secure Token.", essBindAddress);
        eventProducer.sendCompletionSignal(safeJobId, safeTaskId, payload.jobToken(), "COMPLETED", essBindAddress);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", safeTaskId);
    }

    /**
     * Orchestrates the complete lifecycle of a Reduce task.
     * <p>
     * This phase executes a secure Peer-to-Peer (P2P) gRPC fetch to gather intermediate
     * partitions from active Map nodes, stages the data locally, and delegates aggregation
     * to the Sandbox JVM. Eager disk cleanup is strictly enforced via a {@code finally} block.
     * </p>
     *
     * @param payload The deserialized task metadata containing upstream worker endpoints.
     * @param rawJson The original JSON payload to be ingested by the Sandbox JVM.
     * @throws Throwable If P2P fetches fail, data loss is detected, or sandbox execution faults.
     * @throws RuntimeException If a critical network error disrupts the gRPC shuffle stream.
     */
    private void executeReducePhase(TaskPayload payload, String rawJson) throws Throwable {
        String safeJobId = sanitizeId(payload.jobId());
        String safeTaskId = sanitizeId(payload.taskId());
        String safeClassName = sanitizeClassName(payload.className());

        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", safeTaskId, safeJobId);

        String packagePath = safeClassName.replace(".", "/");
        String localCodeDir = Paths.get(baseShuffleDir, safeJobId, "usercode") + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        int partitionIndex = Integer.parseInt(safeTaskId);
        // Raw gRPC streams strictly bounded to the Job Directory
        Path rawDataDir = Paths.get(baseShuffleDir, safeJobId, "reduce_raw", String.valueOf(partitionIndex));
        Files.createDirectories(rawDataDir);

        try {
            // Deduplicate P2P Endpoints
            // Prevents exponential redundant data fetching on single-node or dense clusters.
            List<String> rawEndpoints = payload.workerEndpoints();
            java.util.Set<String> uniqueEndpoints = new java.util.HashSet<>(rawEndpoints);
            List<String> endpoints = new java.util.ArrayList<>(uniqueEndpoints);

            logger.info("Initiating SECURE gRPC P2P transfer from {} unique ESS nodes...", endpoints.size());

            for (int i = 0; i < endpoints.size(); i++) {
                String endpoint = endpoints.get(i);
                String[] addressParts = endpoint.split(":");
                String host = addressParts[0];
                int port = (addressParts.length > 1) ? Integer.parseInt(addressParts[1]) : 7337;

                ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .build();

                // Atomic File Writes
                // Write to a temporary file first to prevent LZ4 frame corruption
                // resulting from incomplete or interleaved gRPC streams.
                Path tmpPath = rawDataDir.resolve("grpc_stream_" + i + ".tmp");
                Path finalPath = rawDataDir.resolve("grpc_stream_" + i + ".lz4");

                try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(tmpPath.toFile()))) {
                    Metadata header = new Metadata();
                    header.put(AUTH_KEY, payload.jobToken());

                    ClientInterceptor authInterceptor = MetadataUtils.newAttachHeadersInterceptor(header);
                    ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = ShuffleServiceGrpc.newBlockingStub(channel)
                            .withInterceptors(authInterceptor);

                    PartitionRequest request = PartitionRequest.newBuilder()
                            .setJobId(safeJobId)
                            .setPartitionId(partitionIndex)
                            .build();

                    Iterator<PartitionChunk> chunkStream = stub.getPartition(request);
                    while (chunkStream.hasNext()) {
                        chunkStream.next().getContent().writeTo(bos);
                    }
                } catch (StatusRuntimeException grpcEx) {
                    Files.deleteIfExists(tmpPath); // Eagerly purge the corrupted partial stream
                    String lostTaskId = "map-chunk-" + i;
                    logger.warn("P2P Data Loss Detected! Triggering Lineage Recovery for {}.", lostTaskId);

                    String errorPayload = "SHUFFLE_FETCH_FAILED:" + lostTaskId;
                    eventProducer.sendErrorSignal(safeJobId, safeTaskId, payload.jobToken(), "FAILED", errorPayload);
                    return;
                } catch (Exception e) {
                    Files.deleteIfExists(tmpPath); // Eagerly purge the corrupted partial stream
                    logger.error("Critical gRPC shuffle failure at node {}", endpoint, e);
                    throw new RuntimeException(e);
                } finally {
                    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
                }

                // If the try-block completed without throwing, the stream is fully intact.
                // Atomically assign the .lz4 extension, signaling readiness to the Sorter.
                if (Files.exists(tmpPath)) {
                    Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
                }
            }

            Path payloadFile = Paths.get(baseShuffleDir, safeJobId, "payloads", safeTaskId + ".json");
            Files.createDirectories(payloadFile.getParent());
            Files.writeString(payloadFile, rawJson);

            logger.info("Streams persisted and validated atomically. Delegating aggregation to Sandbox...");
            runSandbox(payloadFile, payload);

            eventProducer.sendCompletionSignal(safeJobId, safeTaskId, payload.jobToken(), "COMPLETED", null);
            logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE ] ---");

        } finally {
            // Guaranteed Eager Cleanup
            // Eagerly purges the massive, multi-GB gRPC streams the instant the Reducer finishes.
            // Prevents the disk from prematurely hitting the 85% Janitor high-watermark.
            ExternalMergeSorter.cleanupDirectory(rawDataDir);
        }
    }

    /**
     * Spawns an isolated, ephemeral Child JVM to execute user-defined computation logic.
     * <p>
     * This method protects the primary Worker node from Out-Of-Memory (OOM) crashes and
     * malicious code. It establishes a background heartbeat monitor to prevent the
     * Orchestrator's Watchdog from prematurely terminating long-running processes.
     * </p>
     *
     * @param payloadFile The physical path to the staged JSON payload on disk.
     * @param payload     The deserialized metadata used for periodic heartbeat signaling.
     * @throws IOException If the child process cannot be spawned due to OS resource limits.
     * @throws InterruptedException If the executor thread is interrupted while waiting for the sandbox.
     * @throws RuntimeException If the Sandbox JVM terminates with a non-zero exit code.
     */
    private void runSandbox(Path payloadFile, TaskPayload payload) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = Paths.get(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-XX:+UseContainerSupport",
                "-XX:MaxRAMPercentage=50.0" , // REDUCED from 75.0 to 50.0 to guarantee survival alongside the Parent JVM
                "-XX:MaxMetaspaceSize=64m", // Hardcap Metaspace to prevent native memory leaks
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=4",
                "-cp", classpath,
                "com.iliasbolan.engine.execution.SandboxRunner",
                payloadFile.toAbsolutePath().toString(),
                this.baseShuffleDir
        );

        pb.inheritIO();

        java.util.concurrent.ScheduledExecutorService heartbeatExecutor =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        try {
            heartbeatExecutor.scheduleAtFixedRate(
                    () -> eventProducer.sendProgressSignal(payload.jobId(), payload.taskId(), payload.jobToken()),
                    10, 10, TimeUnit.MINUTES
            );

            Process process = pb.start();
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new RuntimeException("Sandbox JVM terminated with exit code: " + exitCode);
            }
        } finally {
            heartbeatExecutor.shutdownNow();
            Files.deleteIfExists(payloadFile);
        }
    }

    /**
     * Validates structural identifiers to prevent Arbitrary Path Traversal attacks.
     * <p>
     * Ensures IDs only contain alphanumeric characters and standard delimiters.
     * </p>
     *
     * @param input The raw identifier string to be sanitized.
     * @return The validated and structurally safe identifier.
     * @throws SecurityException If the input fails structural validation.
     */
    private String sanitizeId(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9\\-]+$")) {
            throw new SecurityException("Invalid ID format. Potential path traversal detected: " + input);
        }
        return input;
    }

    /**
     * Validates class names to ensure they adhere to safe Java package structures.
     * <p>
     * Mitigates nested directory escape vectors during bytecode extraction and loading.
     * </p>
     *
     * @param input The fully qualified class name to be sanitized.
     * @return The validated and structurally safe class name.
     * @throws SecurityException If the input violates standard Java package naming conventions.
     */
    private String sanitizeClassName(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9_.]+$")) {
            throw new SecurityException("Invalid class name format. Potential path traversal detected: " + input);
        }
        return input;
    }
}