package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.messaging.RabbitMqProducer;
import com.iliasbolan.storage.S3ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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
 * <p>
 * This class acts as the critical bridge between the messaging layer (RabbitMQ) and
 * the computation layer. To ensure enterprise-grade stability and security, this
 * orchestrator delegates all untrusted byte-code execution to an isolated Child JVM
 * ({@link SandboxRunner}).
 * </p>
 * <h3>Core Responsibilities:</h3>
 * <ul>
 * <li><b>Deserialization:</b> Converting raw JSON payloads into validated {@link TaskPayload} objects.</li>
 * <li><b>Resource Acquisition:</b> Coordinating with {@link S3ClientService} to localize bytecode and data.</li>
 * <li><b>gRPC P2P Shuffle:</b> Utilizing gRPC Server-Side Streaming to fetch intermediate data.</li>
 * <li><b>Process Isolation (Sandboxing):</b> Spawning ephemeral child processes to execute user code,
 * entirely preventing ClassLoader memory leaks and main-thread security compromises.</li>
 * <li><b>Lifecycle Signaling:</b> Reporting task completion and network coordinates to the Manager.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 5.0
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

    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String podIp) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.podIp = podIp;

        logger.info("Initialized TaskExecutor (Sandbox Orchestrator Mode). Pod IP: {}", podIp);
    }

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
     * """
     * Prepares the environment and delegates Map execution to the Sandbox JVM.
     * * Args:
     * payload (TaskPayload): The metadata required to execute the specific map chunk.
     * rawJson (String): The unparsed JSON string, passed as an argument to the child process.
     * * Raises:
     * Throwable: If an error occurs during resource acquisition or the child JVM fails.
     * """
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
     * """
     * Executes the Reduce phase by streaming remote partition data directly to local disk,
     * then spawning the Sandbox to safely execute the External Merge Sort and user reduction.
     * * Args:
     * payload (TaskPayload): The metadata required to aggregate the intermediate partition.
     * rawJson (String): The unparsed JSON string.
     * * Raises:
     * Throwable: If gRPC network fetching fails, or the child JVM crashes.
     * """
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
     * Spawns an isolated Child JVM to execute untrusted user code.
     *
     * """
     * Bootstraps the SandboxRunner via a native OS ProcessBuilder.
     * * This architectural boundary prevents user-code from accessing the primary worker's
     * memory space, avoiding ClassLoader Metaspace leaks and shielding core orchestration
     * threads from fatal exceptions.
     * * Args:
     * payloadFile (Path): The temporary file containing the task execution instructions.
     * * Raises:
     * RuntimeException: If the child JVM exits with a non-zero failure code.
     * """
     */
    private void runSandbox(Path payloadFile) throws IOException, InterruptedException {
        String javaHome = System.getProperty("java.home");
        String javaBin = Paths.get(javaHome, "bin", "java").toString();
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "com.iliasbolan.engine.SandboxRunner",
                payloadFile.toAbsolutePath().toString(),
                this.baseShuffleDir
        );

        // Inherit IO so the Sandbox's logs are seamlessly piped to the main K8s console
        pb.inheritIO();

        Process process = pb.start();
        int exitCode = process.waitFor();

        // Secure cleanup
        Files.deleteIfExists(payloadFile);

        if (exitCode != 0) {
            throw new RuntimeException("Sandbox JVM terminated abnormally with exit code: " + exitCode);
        }
    }
}