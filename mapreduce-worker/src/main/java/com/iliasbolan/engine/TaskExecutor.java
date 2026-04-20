package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
import com.iliasbolan.core.TaskPayload;
import com.iliasbolan.storage.S3ClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
 * * <h3>Core Responsibilities:</h3>
 * <ul>
 * <li><b>Deserialization:</b> Converting raw JSON payloads into validated {@link TaskPayload} objects.</li>
 * <li><b>Resource Acquisition:</b> Coordinating with {@link S3ClientService} to localize bytecode and data.</li>
 * <li><b>Dynamic Execution:</b> Leveraging Reflection to instantiate user logic at runtime.</li>
 * <li><b>Parallelism:</b> Dispatching workloads to a shared {@link ForkJoinPool} for multi-core utilization.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 1.4
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

    /** Service for S3-compatible storage interactions. */
    private final S3ClientService s3ClientService;

    /** Shared pool for executing parallelized Map and Reduce operations. */
    private final ForkJoinPool forkJoinPool;

    /**
     * Constructs a new {@code TaskExecutor} with a shared parallel execution pool.
     * <p>
     * Note: This constructor initializes the common {@link ForkJoinPool}, which
     * automatically scales based on the available processors in the container.
     * </p>
     *
     * @param s3ClientService The initialized service for S3 storage I/O.
     */
    public TaskExecutor(S3ClientService s3ClientService) {
        this.s3ClientService = s3ClientService;
        this.forkJoinPool = ForkJoinPool.commonPool();

        logger.info("Initialized TaskExecutor with parallel ForkJoinPool (Parallelism level: {})",
                forkJoinPool.getParallelism());
    }

    /**
     * Parses the incoming JSON message and routes it to the correct execution phase.
     * <p>
     * This method acts as the primary entry point for the worker loop. It determines
     * if the workload belongs to a {@code MAP} or {@code REDUCE} phase.
     * </p>
     *
     * @param jsonPayload The raw JSON task description delivered by RabbitMQ.
     * @throws Throwable If any phase of the execution lifecycle fails (I/O, Reflection, or Logic).
     * Throwing bubbles back to the consumer for message NACKing.
     */
    public void executeTask(String jsonPayload) throws Throwable {
        // Deserialize the task definition sent by the Manager
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);
        logger.info("Successfully parsed task payload. JobId: {}, TaskType: {}", payload.jobId(), payload.taskType());

        // Routing logic for the two-phase MapReduce pipeline
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
     * <p>
     * <b>Phase Pipeline:</b>
     * <ol>
     * <li>Localization: Downloads user bytecode to a managed temporary directory.</li>
     * <li>Reflection: Instantiates the {@link Mapper} via a custom ClassLoader.</li>
     * <li>Data Streaming: Fetches boundary-corrected records from S3.</li>
     * <li>Processing: Invokes {@link MapTaskProcessor} for parallel data transformation.</li>
     * <li>Shuffle: Hands results to {@link ShufflePartitioner} for intermediate persistence.</li>
     * </ol>
     * </p>
     *
     * @param payload The metadata required to execute the specific map chunk.
     * @throws Throwable If an error occurs during resource acquisition or data processing.
     */
    private void executeMapPhase(TaskPayload payload) throws Throwable {
        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", payload.taskId());

        // Prepare the local file system for dynamic bytecode loading
        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Reflectively load user logic
        Mapper mapper = DynamicClassLoader.loadMapper(localCodeDir, payload.className());

        // S3 Service handles boundary correction (ensuring lines aren't split across chunks)
        List<String> records = s3ClientService.readDataChunk(
                payload.bucketName(),
                payload.objectName(),
                payload.byteOffset(),
                payload.byteLength()
        );

        logger.info("Successfully received {} clean records from S3 service.", records.size());

        // Parallel processing starts here
        MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper);
        List<KeyValuePair> intermediateResults = forkJoinPool.invoke(rootMapTask);

        logger.info("Parallel Map processing complete. Generated {} intermediate pairs.", intermediateResults.size());

        // Distribute results into partitions for the Reducers to pick up
        ShufflePartitioner partitioner = new ShufflePartitioner(
                s3ClientService,
                payload.bucketName(),
                payload.jobId(),
                payload.taskId(),
                payload.numReducers()
        );
        partitioner.partitionAndUpload(intermediateResults);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", payload.taskId());
    }

    /**
     * Orchestrates the REDUCE task lifecycle with an integrated Sort phase.
     * <p>
     * Unlike the Map phase, the Reduce phase involves a "Grouping and Sorting" stage
     * to provide "Total Order" semantics. This ensures that all values for a given
     * key are processed together and the final output is alphabetically organized.
     * </p>
     *
     * @param payload The metadata required to aggregate the specific intermediate partition.
     * @throws Throwable If aggregation or final output persistence fails.
     */
    private void executeReducePhase(TaskPayload payload) throws Throwable {
        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", payload.taskId(), payload.jobId());

        // bytecode localization (standard procedure)
        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();

        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Reflectively load user reducer logic
        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());

        // Identify all intermediate fragments produced by Map workers for this partition
        int partitionIndex = Integer.parseInt(payload.taskId());
        List<String> files = s3ClientService.listIntermediateFiles(payload.bucketName(), payload.jobId(), partitionIndex);

        // Group intermediate data into a Key -> List<Value> structure
        java.util.Map<String, List<String>> groupedData = new java.util.HashMap<>();
        for (String fileName : files) {
            String content = s3ClientService.readObject(payload.bucketName(), fileName);
            for (String line : content.split("\\n")) {
                if (line.isBlank()) continue;

                String[] parts = line.split("\\t");
                if (parts.length == 2) {
                    groupedData.computeIfAbsent(parts[0], k -> new java.util.ArrayList<>()).add(parts[1]);
                }
            }
        }

        // Apply a deterministic sort to satisfy standard MapReduce ordering requirements
        logger.info("Sorting {} unique intermediate keys for partition {}...", groupedData.size(), partitionIndex);
        List<java.util.Map.Entry<String, List<String>>> sortedEntries = new java.util.ArrayList<>(groupedData.entrySet());
        sortedEntries.sort(java.util.Map.Entry.comparingByKey());

        // Process keys in parallel via the recursive ReduceTaskProcessor
        ReduceTaskProcessor rootReduceTask = new ReduceTaskProcessor(sortedEntries, 0, sortedEntries.size(), reducer);
        List<com.iliasbolan.core.KeyValuePair> finalResults = forkJoinPool.invoke(rootReduceTask);

        logger.info("Parallel Reduction complete. Results remain in sorted order.");

        // Serialize final results to a tab-delimited format
        StringBuilder outputBuilder = new StringBuilder();
        for (com.iliasbolan.core.KeyValuePair pair : finalResults) {
            outputBuilder.append(pair.key()).append("\t").append(pair.value()).append("\n");
        }

        // Final output path is globally accessible to the Manager for job finalization
        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);
        s3ClientService.writeData(payload.bucketName(), finalPath, outputBuilder.toString());

        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: Output saved to {} ] ---", finalPath);
    }
}