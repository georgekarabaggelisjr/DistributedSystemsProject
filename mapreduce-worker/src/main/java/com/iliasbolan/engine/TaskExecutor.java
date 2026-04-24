package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.engine.execution.SandboxRunner;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.services.S3ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
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
 * The central orchestration engine for executing Map-Reduce tasks on the Worker node.
 * * <p>This class acts as the critical bridge between the messaging layer (RabbitMQ) and
 * the computation layer. To ensure enterprise-grade stability and security, this
 * orchestrator delegates all untrusted byte-code execution to an isolated Child JVM
 * ({@link SandboxRunner}).</p>
 * * <h3>Architecture Update: Reactive Lineage Recomputation (The Sentinel)</h3>
 * <p>This worker acts as a Sentinel for Peer-to-Peer ephemeral data loss. During the
 * Shuffle phase, if a remote Map pod is unresponsive (e.g., evicted by Kubernetes),
 * this executor intercepts the gRPC network failure. Instead of crashing, it gracefully
 * suspends execution, signals the exact missing chunk ID to the Orchestrator, and allows
 * the control plane to deterministically rebuild the lost lineage.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 */
public class TaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final S3ClientService s3ClientService;
    private final String baseShuffleDir;
    private final RabbitMqProducer eventProducer;
    private final String podIp;

    /**
     * Constructs a new TaskExecutor with necessary infrastructure dependencies.
     *
     * @param s3ClientService Service for interacting with S3-compatible storage.
     * @param baseShuffleDir  The root local directory used for intermediate data storage.
     * @param eventProducer   Producer for signaling task status to the Manager.
     * @param podIp           The unique IP address of the current Kubernetes pod.
     */
    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String podIp) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.podIp = podIp;

        logger.info("Initialized TaskExecutor (Sandbox Orchestrator Mode). Pod IP: {}", podIp);
    }

    /**
     * Parses a raw JSON task payload and routes it to the appropriate phase handler.
     *
     * @param jsonPayload The serialized {@link TaskPayload} received from the message broker.
     * @throws Throwable If deserialization fails or an unrecoverable error occurs during phase execution.
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
     * Orchestrates the complete lifecycle of a distributed MAP task.
     *
     * <p>This method performs resource localization by downloading the required
     * user-code from S3 and subsequently delegates the Map computation to an
     * ephemeral Sandbox JVM to ensure process isolation.</p>
     *
     * @param payload The metadata required to execute the specific map chunk.
     * @param rawJson The unparsed JSON string, passed as an argument to the child process.
     * @throws Throwable If an error occurs during resource acquisition or the child JVM fails.
     */
    private void executeMapPhase(TaskPayload payload, String rawJson) throws Throwable {
        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", payload.taskId());

        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Persist the payload so the Sandbox JVM can read its instructions
        Path payloadFile = Paths.get("/tmp/mapreduce/payloads", payload.jobId() + "_map_" + payload.taskId() + ".json");
        Files.createDirectories(payloadFile.getParent());
        Files.writeString(payloadFile, rawJson);

        logger.info("Delegating Map computation to Ephemeral Sandbox JVM...");
        runSandbox(payloadFile);

        String grpcPort = System.getenv().getOrDefault("SHUFFLE_GRPC_PORT", "50051");
        String workerBindAddress = this.podIp + ":" + grpcPort;

        eventProducer.sendCompletionSignal(payload.jobId(), payload.taskId(), "COMPLETED", workerBindAddress);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", payload.taskId());
    }

    /**
     * Orchestrates the REDUCE task lifecycle utilizing gRPC and the Sandbox JVM.
     *
     * <p>Executes the Reduce phase by streaming remote partition data directly to local disk.
     * If a remote node is unreachable, it acts as a Sentinel, firing a highly specific
     * telemetry event back to the Manager to trigger Reactive Lineage Recomputation.</p>
     *
     * @param payload The metadata required to aggregate the intermediate partition.
     * @param rawJson The unparsed JSON string.
     * @throws Throwable If a critical, non-network failure occurs.
     */
    private void executeReducePhase(TaskPayload payload, String rawJson) throws Throwable {
        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", payload.taskId(), payload.jobId());

        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        int partitionIndex = Integer.parseInt(payload.taskId());

        // 1. Prepare Local Directory for Raw gRPC Spills
        Path rawDataDir = Paths.get(baseShuffleDir, "reduce_raw", payload.jobId(), String.valueOf(partitionIndex));
        Files.createDirectories(rawDataDir);

        // 2. Fetch data from Map workers via gRPC and Spill directly to Disk
        List<String> endpoints = payload.workerEndpoints();
        logger.info("Initiating gRPC P2P data transfer. Streaming from {} Map workers to disk...", endpoints.size());

        for (int i = 0; i < endpoints.size(); i++) {
            String endpoint = endpoints.get(i);
            String[] addressParts = endpoint.split(":");
            String host = addressParts[0];
            int port = (addressParts.length > 1) ? Integer.parseInt(addressParts[1]) : 50051;

            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .build();

            Path rawSpillFile = rawDataDir.resolve("grpc_stream_" + i + ".txt");

            try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(rawSpillFile.toFile()))) {
                ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = ShuffleServiceGrpc.newBlockingStub(channel);
                PartitionRequest request = PartitionRequest.newBuilder()
                        .setJobId(payload.jobId())
                        .setPartitionId(partitionIndex)
                        .build();

                Iterator<PartitionChunk> chunkStream = stub.getPartition(request);
                while (chunkStream.hasNext()) {
                    PartitionChunk chunk = chunkStream.next();
                    chunk.getContent().writeTo(bos);
                }
            } catch (StatusRuntimeException grpcEx) {
                // --- THE SENTINEL INTERCEPTOR ---
                // We've detected P2P data loss. We dynamically calculate the lost chunk ID based
                // on the deterministic ordering of the endpoints routing table.
                String lostTaskId = "map-chunk-" + i;
                logger.warn("P2P Data Loss Detected! Map pod unreachable at endpoint {}. Triggering Lineage Recovery for '{}'.", endpoint, lostTaskId);

                // Construct a specialized error payload (Requires your RabbitMqProducer to support sending the error field)
                String errorPayload = "SHUFFLE_FETCH_FAILED:" + lostTaskId;

                // Send a targeted failure signal to bypass global Fail-Fast and trigger recovery
                eventProducer.sendErrorSignal(payload.jobId(), payload.taskId(), "FAILED", errorPayload);

                // Gracefully suspend execution WITHOUT crashing the pod.
                // The Orchestrator will rebuild the lost chunk and eventually re-queue this Reducer task.
                return;
            } catch (Exception e) {
                logger.error("gRPC P2P Error: Failed to stream from node {}", endpoint, e);
                throw new RuntimeException("Critical gRPC shuffle failure.", e);
            } finally {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        // 3. Delegate to Sandbox for External Merge Sort & Reduce Execution
        Path payloadFile = Paths.get("/tmp/mapreduce/payloads", payload.jobId() + "_reduce_" + payload.taskId() + ".json");
        Files.createDirectories(payloadFile.getParent());
        Files.writeString(payloadFile, rawJson);

        logger.info("gRPC streams persisted. Delegating External Merge Sort and Reduce to Sandbox JVM...");
        runSandbox(payloadFile);

        // 4. Cleanup local temporary gRPC files (S3 upload was handled by Sandbox)
        ExternalMergeSorter.cleanupDirectory(rawDataDir);

        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);
        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: {} ] ---", finalPath);
    }

    /**
     * Spawns an isolated Child JVM to execute untrusted user code via {@link ProcessBuilder}.
     *
     * <p>This architectural boundary prevents user-code from accessing the primary
     * worker's memory space, avoiding ClassLoader Metaspace leaks and shielding
     * core orchestration threads from fatal exceptions.</p>
     *
     * @param payloadFile The temporary file containing the task execution instructions.
     * @throws IOException          If the OS fails to allocate process resources.
     * @throws InterruptedException If the K8s node preempts the worker pod during execution.
     * @throws RuntimeException     If the child JVM exits with a non-zero failure code.
     */
    private void runSandbox(Path payloadFile) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = Paths.get(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "com.iliasbolan.engine.execution.SandboxRunner",
                payloadFile.toAbsolutePath().toString(),
                this.baseShuffleDir
        );

        // Inherit IO so the Sandbox's logs are seamlessly piped to the main K8s console
        pb.inheritIO();

        Process process = pb.start();
        int exitCode = process.waitFor();

        // Secure cleanup of the temporary instruction file
        Files.deleteIfExists(payloadFile);

        if (exitCode != 0) {
            throw new RuntimeException("Sandbox JVM terminated abnormally with exit code: " + exitCode);
        }
    }
}