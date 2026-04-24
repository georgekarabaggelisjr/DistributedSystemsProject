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
 * <li><b>Security (Sandboxing):</b> Malicious or poorly written user scripts cannot crash the main worker,
 * access its sensitive memory space, or compromise primary event loops.</li>
 * <li><b>Memory Management (ClassLoader Leaks):</b> Because the {@link DynamicClassLoader}
 * operates exclusively within this ephemeral JVM, all Metaspace, loaded classes, and static
 * references are completely obliterated by the operating system when this process exits,
 * guaranteeing zero memory leaks across thousands of tasks.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-24
 */
public class SandboxRunner {

    private static final Logger logger = LoggerFactory.getLogger(SandboxRunner.class);

    /**
     * High-performance JSON serializer used for instruction extraction.
     * Configured to ignore unknown properties for cross-version compatibility.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * The primary execution hook for the Child JVM.
     *
     * <p>Bootstraps the isolated environment, parses task instructions, dynamically loads
     * user bytecode, and executes the designated computation pipeline.</p>
     *
     * @param args Command line arguments:
     * <ul>
     * <li>args[0]: Absolute path to the JSON file containing the serialized {@link TaskPayload}.</li>
     * <li>args[1]: Base directory for local shuffle data persistence.</li>
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

            // Establish a temporary isolated connection for data retrieval
            // Note: In a true zero-trust sandbox, the main worker would download the data
            // and pass local file paths to the sandbox. For Phase 2, we allow the sandbox to fetch.
            MinioConnectionManager manager = new MinioConnectionManager(
                    System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000"),
                    System.getenv().getOrDefault("MINIO_ACCESS_KEY", "minioadmin"),
                    System.getenv().getOrDefault("MINIO_SECRET_KEY", "minioadmin")
            );
            S3ClientService s3ClientService = new S3ClientService(manager);

            String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";

            if ("MAP".equalsIgnoreCase(payload.taskType())) {
                executeMap(payload, localCodeDir, baseShuffleDir, s3ClientService);
            } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
                executeReduce(payload, localCodeDir, baseShuffleDir, s3ClientService);
            } else {
                throw new IllegalArgumentException("Unsupported task type in Sandbox: " + payload.taskType());
            }

            // Normal termination: OS reclaims all memory
            logger.info("Sandbox execution completed successfully. Terminating JVM.");
            System.exit(0);

        } catch (Throwable t) {
            logger.error("FATAL: Sandbox JVM encountered an unrecoverable exception.", t);
            System.exit(1); // Non-zero exit code signals failure to the master Orchestrator
        }
    }

    /**
     * Orchestrates the execution of a MAP task within the sandbox environment.
     *
     * @param payload        The instructions and metadata for the Map task.
     * @param localCodeDir   Local directory containing the dynamically loaded user code.
     * @param baseShuffleDir Local directory for persisting intermediate shuffle data.
     * @param s3             Service for interacting with S3-compatible storage.
     * @throws Throwable If class loading, data retrieval, or parallel execution fails.
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
     * Orchestrates the execution of a REDUCE task within the sandbox environment.
     *
     * @param payload        The instructions and metadata for the Reduce task.
     * @param localCodeDir   Local directory containing the dynamically loaded user code.
     * @param baseShuffleDir Local directory for retrieving intermediate shuffle data.
     * @param s3             Service for persisting final output to shared storage.
     * @throws Throwable If class loading, merge-sort operations, or data persistence fails.
     */
    private static void executeReduce(TaskPayload payload, String localCodeDir, String baseShuffleDir, S3ClientService s3) throws Throwable {
        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());
        int partitionIndex = Integer.parseInt(payload.taskId());

        Path rawDataDir = Paths.get(baseShuffleDir, "reduce_raw", payload.jobId(), String.valueOf(partitionIndex));

        // Execute the K-Way Merge Sort utilizing the isolated classloader's Reducer
        Path finalReducedFile = ExternalMergeSorter.sortReduceAndSpill(rawDataDir, reducer, ForkJoinPool.commonPool());

        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);
        s3.writeData(payload.bucketName(), finalPath, Files.readString(finalReducedFile));
    }
}