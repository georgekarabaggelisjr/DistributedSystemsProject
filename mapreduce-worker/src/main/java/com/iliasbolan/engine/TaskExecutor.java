package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.storage.S3ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

/**
 * The central orchestration engine for executing Map-Reduce tasks on the Worker node.
 * <p>
 * This class acts as the critical bridge between the messaging layer (RabbitMQ) and
 * the computation layer (JVM Threads). It handles the full lifecycle of a task
 * execution, ensuring that remote resources are localized and processed via
 * parallel decomposition.
 * </p>
 * <h3>Core Responsibilities:</h3>
 * <ul>
 * <li><b>Deserialization:</b> Converting raw JSON payloads into validated {@link TaskPayload} objects.</li>
 * <li><b>Resource Acquisition:</b> Coordinating with {@link S3ClientService} to localize bytecode and data.</li>
 * <li><b>Dynamic Execution:</b> Leveraging Reflection to instantiate user logic at runtime.</li>
 * <li><b>Parallelism:</b> Dispatching workloads to a shared {@link ForkJoinPool} for multi-core utilization.</li>
 * <li><b>Peer-to-Peer (P2P) Fetching:</b> Utilizing native Java HTTP clients to stream intermediate shuffle data directly from sibling nodes, bypassing S3 bottlenecks.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-07
 * @see com.iliasbolan.core.TaskPayload
 * @see com.iliasbolan.engine.MapTaskProcessor
 * @see com.iliasbolan.engine.ShufflePartitioner
 */
public class TaskExecutor {

    /** Logger instance for tracking task execution and system health. */
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    /** High-performance JSON mapper for processing RabbitMQ message bodies. */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Service for S3-compatible storage interactions (Used for Code/Final Output). */
    private final S3ClientService s3ClientService;

    /** Shared pool for executing parallelized Map and Reduce operations. */
    private final ForkJoinPool forkJoinPool;

    /** The root directory for local P2P shuffle data persistence. */
    private final String baseShuffleDir;

    /**
     * Constructs a new {@code TaskExecutor} with a shared parallel execution pool.
     * <p>
     * Note: This constructor initializes the common {@link ForkJoinPool}, which
     * automatically scales based on the available processors in the container.
     * </p>
     *
     * @param s3ClientService The initialized service for S3 storage I/O.
     * @param baseShuffleDir  The local disk directory for hosting P2P intermediate data.
     */
    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.forkJoinPool = ForkJoinPool.commonPool();

        logger.info("Initialized TaskExecutor with parallel ForkJoinPool (Parallelism level: {})",
                forkJoinPool.getParallelism());
    }

    /**
     * Parses the incoming JSON message and routes it to the correct execution phase.
     *
     * @param jsonPayload The raw JSON task description delivered by RabbitMQ.
     * @throws Throwable If any phase of the execution lifecycle fails.
     */
    public void executeTask(String jsonPayload) throws Throwable {
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);
        logger.info("Successfully parsed task payload. JobId: {}, TaskType: {}", payload.jobId(), payload.taskType());

        if ("MAP".equalsIgnoreCase(payload.taskType())) {
            executeMapPhase(payload);
        } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
            executeReducePhase(payload);
        } else {
            throw new IllegalArgumentException("Unknown task type received: " + payload.taskType());
        }
    }

    /**
     * Orchestrates the complete lifecycle of a distributed MAP task.
     *
     * @param payload The metadata required to execute the specific map chunk.
     * @throws Throwable If an error occurs during resource acquisition or data processing.
     */
    private void executeMapPhase(TaskPayload payload) throws Throwable {
        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", payload.taskId());

        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        Mapper mapper = DynamicClassLoader.loadMapper(localCodeDir, payload.className());

        List<String> records = s3ClientService.readDataChunk(
                payload.bucketName(),
                payload.objectName(),
                payload.byteOffset(),
                payload.byteLength()
        );

        logger.info("Successfully received {} clean records from S3 service.", records.size());

        MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper);
        List<KeyValuePair> intermediateResults = forkJoinPool.invoke(rootMapTask);

        logger.info("Parallel Map processing complete. Generated {} intermediate pairs.", intermediateResults.size());

        // P2P UPGRADE: Using local disk storage instead of S3 uploads
        ShufflePartitioner partitioner = new ShufflePartitioner(
                baseShuffleDir,
                payload.jobId(),
                payload.taskId(),
                payload.numReducers()
        );
        partitioner.partitionAndWriteLocal(intermediateResults);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", payload.taskId());
    }

    /**
     * Orchestrates the REDUCE task lifecycle utilizing direct P2P data fetching.
     * <p>
     * <b>P2P Shuffle Protocol:</b><br>
     * This method retrieves the routing table ({@code payload.workerEndpoints()}) and initiates
     * HTTP GET requests to sibling pods. It requests the entire partition directory, streaming
     * the intermediate text data directly into memory for grouping and sorting.
     * </p>
     *
     * @param payload The metadata required to aggregate the specific intermediate partition.
     * @throws Throwable If P2P network fetching, aggregation, or final output persistence fails.
     */
    private void executeReducePhase(TaskPayload payload) throws Throwable {
        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", payload.taskId(), payload.jobId());

        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());

        int partitionIndex = Integer.parseInt(payload.taskId());
        java.util.Map<String, List<String>> groupedData = new java.util.HashMap<>();

        // P2P SHUFFLE PHASE: Fetch data directly from Map worker endpoints
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        List<String> endpoints = payload.workerEndpoints();

        logger.info("Initiating P2P data transfer. Fetching partition {} from {} active Map workers...", partitionIndex, endpoints.size());

        for (String endpoint : endpoints) {
            String url = String.format("http://%s/shuffle/%s/%d", endpoint, payload.jobId(), partitionIndex);
            try {
                HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    String content = response.body();
                    for (String line : content.split("\\n")) {
                        if (line.isBlank()) continue;
                        String[] parts = line.split("\\t");
                        if (parts.length == 2) {
                            groupedData.computeIfAbsent(parts[0], k -> new java.util.ArrayList<>()).add(parts[1]);
                        }
                    }
                } else if (response.statusCode() == 404) {
                    logger.debug("Worker node {} does not contain data for partition {}.", endpoint, partitionIndex);
                } else {
                    logger.warn("Received unexpected status code {} from P2P node: {}", response.statusCode(), url);
                }
            } catch (Exception e) {
                logger.error("P2P Network Error: Failed to fetch partition data from node {}", endpoint, e);
                throw new RuntimeException("Critical P2P network failure during Reduce phase.", e);
            }
        }

        logger.info("Sorting {} unique intermediate keys for partition {}...", groupedData.size(), partitionIndex);
        List<java.util.Map.Entry<String, List<String>>> sortedEntries = new java.util.ArrayList<>(groupedData.entrySet());
        sortedEntries.sort(java.util.Map.Entry.comparingByKey());

        ReduceTaskProcessor rootReduceTask = new ReduceTaskProcessor(sortedEntries, 0, sortedEntries.size(), reducer);
        List<com.iliasbolan.core.KeyValuePair> finalResults = forkJoinPool.invoke(rootReduceTask);

        logger.info("Parallel Reduction complete. Results remain in sorted order.");

        StringBuilder outputBuilder = new StringBuilder();
        for (com.iliasbolan.core.KeyValuePair pair : finalResults) {
            outputBuilder.append(pair.key()).append("\t").append(pair.value()).append("\n");
        }

        // Final output is still pushed to S3 so the user can download their result
        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);
        s3ClientService.writeData(payload.bucketName(), finalPath, outputBuilder.toString());

        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: Output saved to {} ] ---", finalPath);
    }
}