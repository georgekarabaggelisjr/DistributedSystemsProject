package com.iliasbolan.engine.execution;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.engine.shuffle.ExternalMergeSorter;
import com.iliasbolan.engine.shuffle.ShufflePartitioner;
import com.iliasbolan.services.S3ClientService;
import com.iliasbolan.infrastructure.MinioConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * The isolated entry point for executing user-defined Map-Reduce logic.
 *
 * <p><b>Enterprise Sandboxing & Resource Isolation:</b><br>
 * This class is designed to be executed as a standalone Child JVM process spawned by the
 * primary Worker daemon. By physically isolating the execution of third-party bytecode,
 * the system achieves several critical architectural goals:</p>
 * <ul>
 * <li><b>Security (Sandboxing):</b> Malicious or poorly written user scripts cannot crash the main worker.</li>
 * <li><b>Memory Management (ClassLoader Leaks):</b> All Metaspace and loaded classes are completely
 * obliterated by the OS when this process exits, guaranteeing zero memory leaks across thousands of tasks.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 2.1
 * @since 2026-04-24
 */
public class SandboxRunner {

    /** The SLF4J logger instance for recording sandbox lifecycle events and fatal JVM errors. */
    private static final Logger logger = LoggerFactory.getLogger(SandboxRunner.class);

    /** High-performance JSON serializer configured to ignore unknown properties for resilient deserialization. */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * The primary execution hook for the isolated Child JVM.
     * <p>
     * Bootstraps the ephemeral environment, parses the injected task instructions,
     * establishes an independent connection to the S3 storage layer, and routes
     * execution to the appropriate Map or Reduce phase handler.
     * </p>
     *
     * @param args Command line arguments injected by the parent {@code TaskExecutor}:
     * <ul>
     * <li>{@code args[0]}: Absolute path to the serialized JSON {@link TaskPayload} file.</li>
     * <li>{@code args[1]}: The root physical directory for ephemeral shuffle data bounding.</li>
     * </ul>
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            logger.error("CRITICAL: SandboxRunner requires exactly 2 arguments: <PayloadFilePath> <BaseShuffleDir>");
            System.exit(1);
        }

        try {
            Path payloadFile = Paths.get(args[0]);
            String baseShuffleDir = args[1];

            logger.info("Initializing Ephemeral Sandbox JVM. Payload: {}", payloadFile);

            String jsonPayload = Files.readString(payloadFile);
            TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);

            MinioConnectionManager manager = new MinioConnectionManager(
                    System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000"),
                    System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin"),
                    System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin")
            );
            S3ClientService s3ClientService = new S3ClientService(manager);

            // Unified pathing ensures StorageJanitor automatically cleans user code
            String localCodeDir = Paths.get(baseShuffleDir, payload.jobId(), "usercode") + "/";

            if ("MAP".equalsIgnoreCase(payload.taskType())) {
                executeMap(payload, localCodeDir, baseShuffleDir, s3ClientService);
            } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
                executeReduce(payload, localCodeDir, baseShuffleDir, s3ClientService);
            } else {
                throw new IllegalArgumentException("Unsupported task type in Sandbox: " + payload.taskType());
            }

            logger.info("Sandbox execution completed successfully. Terminating JVM.");
            System.exit(0);

        } catch (Throwable t) {
            logger.error("FATAL: Sandbox JVM encountered an unrecoverable exception.", t);
            System.exit(1);
        }
    }

    /**
     * Orchestrates the execution of a Map task within the isolated sandbox boundary.
     * <p>
     * Dynamically loads the user's {@link Mapper} implementation, streams a bounded
     * chunk of records from S3 into memory, and submits the workload to the
     * {@link ForkJoinPool} for highly parallelized, work-stealing execution.
     * </p>
     *
     * @param payload        The deserialized task instructions and metadata.
     * @param localCodeDir   The local directory containing the dynamically loaded user bytecode.
     * @param baseShuffleDir The root local directory for persisting intermediate partitioned data.
     * @param s3             The configured service client for interacting with S3-compatible storage.
     * @throws Throwable If class loading, data retrieval, or parallel execution encounters a fatal error.
     */
    private static void executeMap(TaskPayload payload, String localCodeDir, String baseShuffleDir, S3ClientService s3) throws Throwable {
        Mapper mapper = DynamicClassLoader.loadMapper(localCodeDir, payload.className());

        List<String> records = s3.readDataChunk(
                payload.bucketName(), payload.objectName(), payload.byteOffset(), payload.byteLength());

        ShufflePartitioner partitioner = new ShufflePartitioner(
                baseShuffleDir, payload.jobId(), payload.taskId(), payload.numReducers());

        MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper, partitioner);
        ForkJoinPool.commonPool().invoke(rootMapTask);
    }

    /**
     * Orchestrates the execution of a Reduce task within the isolated sandbox boundary.
     * <p>
     * Dynamically loads the user's {@link Reducer} implementation, initiates the
     * O(1) memory External Merge Sort on the raw gRPC data streams, and securely
     * streams the final aggregated results directly to S3, bypassing the JVM heap.
     * </p>
     *
     * @param payload        The deserialized task instructions and metadata.
     * @param localCodeDir   The local directory containing the dynamically loaded user bytecode.
     * @param baseShuffleDir The root local directory used to resolve the job's ephemeral boundary.
     * @param s3             The configured service client for persisting final output to shared storage.
     * @throws Throwable If class loading, K-Way merge-sort operations, or data persistence fails.
     */
    private static void executeReduce(TaskPayload payload, String localCodeDir, String baseShuffleDir, S3ClientService s3) throws Throwable {
        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());
        int partitionIndex = Integer.parseInt(payload.taskId());

        // Raw Data strictly isolated within the Job's directory boundary for Janitor collection
        Path rawDataDir = Paths.get(baseShuffleDir, payload.jobId(), "reduce_raw", String.valueOf(partitionIndex));

        Path finalReducedFile = ExternalMergeSorter.sortReduceAndSpill(rawDataDir, reducer, ForkJoinPool.commonPool());

        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);

        // O(1) memory upload. Streams directly from disk, preventing "Finish-Line" OOM crashes.
        s3.uploadFileFromDisk(payload.bucketName(), finalPath, finalReducedFile);
    }
}