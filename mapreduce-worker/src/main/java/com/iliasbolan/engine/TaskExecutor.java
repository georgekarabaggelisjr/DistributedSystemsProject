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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The central orchestration engine for executing Map-Reduce tasks on a Worker node.
 * <p>
 * This class serves as the primary controller for the Worker's lifecycle, managing
 * the transition between Map and Reduce phases. It handles resource localization
 * from S3, gRPC-based peer-to-peer data transfers, and delegates heavy computational
 * logic to an isolated Sandbox JVM to ensure worker stability.
 * </p>
 * <p>
 * <b>Architecture Highlight: Secure Data Plane (Token Authentication).</b><br>
 * For zero-trust security, this executor captures a 'jobToken' issued by the Manager
 * and injects it into gRPC metadata during shuffle fetches. This ensures that the
 * External Shuffle Service (ESS) only serves data to authorized Reducer pods belonging
 * to the same job.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-25
 */
public class TaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    /**
     * Pre-configured Jackson mapper for robust JSON deserialization of task intents.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final S3ClientService s3ClientService;
    private final String baseShuffleDir;
    private final RabbitMqProducer eventProducer;
    private final String nodeIp;

    /**
     * Global gRPC Metadata key for transmitting the Job Authorization Token.
     */
    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Constructs a new TaskExecutor with the required infrastructure services.
     *
     * @param s3ClientService Client for downloading user-provided JARs and class files.
     * @param baseShuffleDir  Root local directory for persisting intermediate shuffle data.
     * @param eventProducer   Producer for signaling task status (COMPLETED/FAILED) back to the Manager.
     * @param nodeIp          The physical IP of the host node, used for data locality signaling.
     */
    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String nodeIp) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.nodeIp = nodeIp;
    }

    /**
     * Parses a raw JSON task payload and routes it to the appropriate execution phase.
     *
     * @param jsonPayload The serialized {@link TaskPayload} containing job metadata and task intents.
     * @throws Throwable If deserialization fails or if an unrecoverable error occurs during execution.
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
     * Handles the Map phase lifecycle: localizing code, running the logic, and signaling data availability.
     *
     * @param payload Pre-parsed task metadata.
     * @param rawJson The original payload to be passed into the Sandbox JVM.
     * @throws Throwable If code download or sandbox execution fails.
     */
    private void executeMapPhase(TaskPayload payload, String rawJson) throws Throwable {
        String safeJobId = sanitizeId(payload.jobId());
        String safeTaskId = sanitizeId(payload.taskId());
        String safeClassName = sanitizeClassName(payload.className());

        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", safeTaskId);

        // Step 1: Localize the user-provided Mapper class safely
        String packagePath = safeClassName.replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + safeJobId + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Step 2: Persist the task payload for Sandbox ingestion
        Path payloadFile = Paths.get("/tmp/mapreduce/payloads", safeJobId + "_map_" + safeTaskId + ".json");
        Files.createDirectories(payloadFile.getParent());
        Files.writeString(payloadFile, rawJson);

        // Step 3: Run the user code in an isolated process
        logger.info("Delegating Map computation to Ephemeral Sandbox JVM...");
        runSandbox(payloadFile, payload);

        if (this.nodeIp == null || this.nodeIp.trim().isEmpty()) {
            throw new IllegalStateException("CRITICAL FATAL: NODE_IP environment variable missing.");
        }

        // Step 4: Broadcast locality hint (HOSTNAME@@IP) to the Manager
        String nodeName = System.getenv().getOrDefault("NODE_NAME", "unknown-node");
        String essBindAddress = nodeName + "@@" + this.nodeIp + ":7337";

        logger.info("Map finished. Signaling ESS Endpoint: {} with Secure Token.", essBindAddress);
        eventProducer.sendCompletionSignal(safeJobId, safeTaskId, payload.jobToken(), "COMPLETED", essBindAddress);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", safeTaskId);
    }

    /**
     * Handles the Reduce phase lifecycle: fetching partition data across nodes and performing final aggregation.
     *
     * @param payload Pre-parsed task metadata.
     * @param rawJson The original payload to be passed into the Sandbox JVM.
     * @throws Throwable If gRPC streaming or sandbox aggregation fails.
     */
    private void executeReducePhase(TaskPayload payload, String rawJson) throws Throwable {
        String safeJobId = sanitizeId(payload.jobId());
        String safeTaskId = sanitizeId(payload.taskId());
        String safeClassName = sanitizeClassName(payload.className());

        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", safeTaskId, safeJobId);

        // Step 1: Localize the Reducer class safely
        String packagePath = safeClassName.replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + safeJobId + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        int partitionIndex = Integer.parseInt(safeTaskId);
        Path rawDataDir = Paths.get(baseShuffleDir, "reduce_raw", safeJobId, String.valueOf(partitionIndex));
        Files.createDirectories(rawDataDir);

        List<String> endpoints = payload.workerEndpoints();
        logger.info("Initiating SECURE gRPC P2P transfer from {} ESS nodes...", endpoints.size());

        // Step 2: Peer-to-Peer shuffle fetch
        for (int i = 0; i < endpoints.size(); i++) {
            String endpoint = endpoints.get(i);
            String[] addressParts = endpoint.split(":");
            String host = addressParts[0];
            int port = (addressParts.length > 1) ? Integer.parseInt(addressParts[1]) : 7337;

            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .build();

            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(rawDataDir.resolve("grpc_stream_" + i + ".txt").toFile()))) {

                // Inject security headers using a client interceptor
                Metadata header = new Metadata();
                header.put(AUTH_KEY, payload.jobToken());

                ClientInterceptor authInterceptor = MetadataUtils.newAttachHeadersInterceptor(header);

                // Create a secure stub for the shuffle fetch
                ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = ShuffleServiceGrpc.newBlockingStub(channel)
                        .withInterceptors(authInterceptor);

                PartitionRequest request = PartitionRequest.newBuilder()
                        .setJobId(safeJobId)
                        .setPartitionId(partitionIndex)
                        .build();

                // Stream intermediate data chunks directly to disk
                Iterator<PartitionChunk> chunkStream = stub.getPartition(request);
                while (chunkStream.hasNext()) {
                    chunkStream.next().getContent().writeTo(bos);
                }
            } catch (StatusRuntimeException grpcEx) {
                // Signal partial data loss to trigger orchestrator recovery (Lineage Recovery)
                String lostTaskId = "map-chunk-" + i;
                logger.warn("P2P Data Loss Detected! Triggering Lineage Recovery for {}.", lostTaskId);

                String errorPayload = "SHUFFLE_FETCH_FAILED:" + lostTaskId;
                eventProducer.sendErrorSignal(safeJobId, safeTaskId, payload.jobToken(), "FAILED", errorPayload);
                return;
            } catch (Exception e) {
                logger.error("Critical gRPC shuffle failure at node {}", endpoint, e);
                throw new RuntimeException(e);
            } finally {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        // Step 3: Run the Reducer sandbox to aggregate the fetched streams
        Path payloadFile = Paths.get("/tmp/mapreduce/payloads", safeJobId + "_reduce_" + safeTaskId + ".json");
        Files.createDirectories(payloadFile.getParent());
        Files.writeString(payloadFile, rawJson);

        logger.info("Streams persisted. Delegating aggregation to Sandbox...");
        runSandbox(payloadFile, payload);

        // Step 4: Reclamation of local shuffle space
        ExternalMergeSorter.cleanupDirectory(rawDataDir);

        eventProducer.sendCompletionSignal(safeJobId, safeTaskId, payload.jobToken(), "COMPLETED", null);
        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE ] ---");
    }

    /**
     * Executes the user code in a dedicated JVM process to protect the worker from heap crashes,
     * while maintaining a continuous heartbeat to the Orchestrator.
     *
     * @param payloadFile The JSON task manifest which the sandbox JVM will parse.
     * @param payload     The deserialized task metadata used for heartbeat signaling.
     * @throws IOException          If the sandbox process cannot be started.
     * @throws InterruptedException If the executor is interrupted while waiting for the sandbox.
     */
    private void runSandbox(Path payloadFile, TaskPayload payload) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = Paths.get(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        // Spawn child JVM with inherited classpath and custom sandbox entry point
        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "com.iliasbolan.engine.execution.SandboxRunner",
                payloadFile.toAbsolutePath().toString(),
                this.baseShuffleDir
        );

        pb.inheritIO();

        // --- ARCHITECTURAL FIX: Long-Running Task Heartbeats ---
        java.util.concurrent.ScheduledExecutorService heartbeatExecutor =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor();

        try {
            // Schedule the heartbeat to fire every 10 minutes
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
            // Guarantee the background thread terminates when the task finishes or crashes
            heartbeatExecutor.shutdownNow();

            // Ensure temporary manifests are purged
            Files.deleteIfExists(payloadFile);
        }
    }

    /**
     * Validates structural identifiers to prevent Arbitrary Path Traversal attacks.
     * Ensures IDs only contain alphanumeric characters and standard delimiters.
     *
     * @param input The id to be sanitized.
     * @return The sanitized id.
     */
    private String sanitizeId(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9\\-]+$")) {
            throw new SecurityException("Invalid ID format. Potential path traversal detected: " + input);
        }
        return input;
    }

    /**
     * Validates class names to ensure they adhere to safe Java package structures,
     * mitigating nested directory escapes during file extraction.
     *
     * @param input The class name to be sanitized.
     * @return The sanitized class name.
     */
    private String sanitizeClassName(String input) {
        if (input == null || !input.matches("^[a-zA-Z0-9_.]+$")) {
            throw new SecurityException("Invalid class name format. Potential path traversal detected: " + input);
        }
        return input;
    }
}