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
 * This class acts as the bridge between the messaging layer and the computation layer.
 * It is responsible for:
 * </p>
 * <ul>
 * <li><b>Deserialization:</b> Converting the raw JSON payload from RabbitMQ into a structured {@link TaskPayload} object using Jackson.</li>
 * <li><b>Resource Acquisition:</b> Utilizing the {@link S3ClientService} to download the designated data chunks and user-compiled code from MinIO.</li>
 * <li><b>Dynamic Execution:</b> Loading the user's logic via Reflection and dispatching the workload to the {@link ForkJoinPool}.</li>
 * <li><b>Output Routing:</b> Passing the intermediate results to the {@link ShufflePartitioner} for proper routing.</li>
 * </ul>
 * * <p>
 * <b>Thread Safety:</b> This executor is designed to be instantiated once per worker and can safely handle
 * sequential task execution within the RabbitMQ consumer's thread.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @see com.iliasbolan.core.TaskPayload
 * @see com.iliasbolan.engine.MapTaskProcessor
 * @see com.iliasbolan.engine.ShufflePartitioner
 * @since 2026-04-07
 */
public class TaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final S3ClientService s3ClientService;
    private final ForkJoinPool forkJoinPool;

    /**
     * Constructs a new {@code TaskExecutor}.
     *
     * @param s3ClientService The initialized service for interacting with the MinIO shared file system.
     */
    public TaskExecutor(S3ClientService s3ClientService) {
        this.s3ClientService = s3ClientService;
        // We use the common pool which automatically sizes itself to the container's available cores!
        this.forkJoinPool = ForkJoinPool.commonPool();

        logger.info("Initialized TaskExecutor with parallel ForkJoinPool (Parallelism level: {})",
                forkJoinPool.getParallelism());
    }

    /**
     * Parses the incoming JSON message and routes it to the appropriate execution phase (Map or Reduce).
     *
     * @param jsonPayload The raw JSON string delivered by RabbitMQ.
     * @throws Exception If any step of the execution fails (parsing, downloading, processing, or uploading).
     * This exception is intended to bubble up to the consumer to trigger a NACK.
     */
    public void executeTask(String jsonPayload) throws Exception {
        // 1. Deserialize the JSON string into our Java Record
        TaskPayload payload = objectMapper.readValue(jsonPayload, TaskPayload.class);
        logger.info("Successfully parsed task payload. JobId: {}, TaskType: {}", payload.jobId(), payload.taskType());

        // 2. Route the task based on its type
        if ("MAP".equalsIgnoreCase(payload.taskType())) {
            executeMapPhase(payload);
        } else if ("REDUCE".equalsIgnoreCase(payload.taskType())) {
            executeReducePhase(payload);
        } else {
            throw new IllegalArgumentException("Unknown task type received: " + payload.taskType());
        }
    }

    /**
     * Orchestrates the complete lifecycle of a MAP task.
     * <p>
     * <b>Execution Flow:</b>
     * <ol>
     * <li>Downloads the user's compiled {@code .class} file to a local temporary directory.</li>
     * <li>Loads the class dynamically using {@link DynamicClassLoader}.</li>
     * <li>Fetches the exact byte-range (e.g., 64MB chunk) of the input data from MinIO.</li>
     * <li>Splits the raw text into individual records (lines).</li>
     * <li>Invokes the {@link MapTaskProcessor} to compute the chunk in parallel.</li>
     * <li>Passes the results to the {@link ShufflePartitioner} to upload the intermediate files.</li>
     * </ol>
     * </p>
     *
     * @param payload The structured data containing all routing and execution metadata.
     * @throws Exception If any operational boundary fails.
     */
    private void executeMapPhase(TaskPayload payload) throws Exception {
        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", payload.taskId());

        // Step 1: Download User Code and reconstruct package directories
        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        // Ensure the nested directories exist before downloading!
        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();

        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Step 2: Dynamically load the Mapper (pointing ClassLoader to the root code dir)
        Mapper mapper = DynamicClassLoader.loadMapper(localCodeDir, payload.className());

        // Step 3: Fetch the data chunk from MinIO
        String rawChunkData = s3ClientService.readDataChunk(
                payload.bucketName(),
                payload.objectName(),
                payload.byteOffset(),
                payload.byteLength()
        );

        // Step 4: Split the raw text into individual lines (records)
        // Using \r?\n handles both Windows and Linux line endings safely
        List<String> records = Arrays.asList(rawChunkData.split("\\r?\\n"));
        logger.info("Successfully split chunk into {} records.", records.size());

        // Step 5: Execute the Parallel Map Task
        MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper);
        List<KeyValuePair> intermediateResults = forkJoinPool.invoke(rootMapTask);

        logger.info("Parallel Map processing complete. Generated {} intermediate pairs.", intermediateResults.size());

        // Step 6: Shuffle and Partition
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
     * Orchestrates the complete lifecycle of a REDUCE task with an integrated Sort phase.
     * <p>
     * <b>Architectural Upgrade:</b><br>
     * This implementation introduces a deterministic Sort phase between the Data Ingestion
     * and Parallel Execution stages. By sorting the intermediate keys, we provide
     * "Total Order" semantics, ensuring the final output files are alphabetically
     * organized—a requirement for many downstream analytical tools.
     * </p>
     *
     * @param payload The structured data containing all routing and execution metadata.
     * @throws Exception If any operational boundary fails (I/O, Reflection, or Network).
     */
    private void executeReducePhase(TaskPayload payload) throws Exception {
        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", payload.taskId(), payload.jobId());

        // Step 1: Resource Acquisition (User Code) and reconstruct package directories
        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        // Ensure the nested directories exist before downloading!
        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();

        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Load Reducer (pointing ClassLoader to the root code dir)
        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());

        // Step 2: Fragment Discovery
        int partitionIndex = Integer.parseInt(payload.taskId());
        List<String> files = s3ClientService.listIntermediateFiles(payload.bucketName(), payload.jobId(), partitionIndex);

        // Step 3: Data Ingestion & Grouping
        // We use a HashMap for initial grouping as it provides O(1) insertion performance.
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

        // Step 4: Deterministic Sorting
        // To enable parallel processing, we convert the map to an ArrayList.
        // We then apply a Sort to the entire keyset to satisfy Map-Reduce sorting requirements.
        logger.info("Sorting {} unique intermediate keys for partition {}...", groupedData.size(), partitionIndex);

        List<java.util.Map.Entry<String, List<String>>> sortedEntries = new java.util.ArrayList<>(groupedData.entrySet());

        // Perform an in-place sort using the key's natural (alphabetical) order
        sortedEntries.sort(java.util.Map.Entry.comparingByKey());

        // Step 5: Parallel Execution
        // The sorted list is now passed to the Fork/Join pool.
        // Since the list is sorted, each sub-task in the pool handles a contiguous "range" of keys.
        ReduceTaskProcessor rootReduceTask = new ReduceTaskProcessor(sortedEntries, 0, sortedEntries.size(), reducer);
        List<com.iliasbolan.core.KeyValuePair> finalResults = forkJoinPool.invoke(rootReduceTask);

        logger.info("Parallel Reduction complete. Results remain in sorted order.");

        // Step 6: Final Persistence (Output will now be alphabetically sorted by key)
        StringBuilder outputBuilder = new StringBuilder();
        for (com.iliasbolan.core.KeyValuePair pair : finalResults) {
            outputBuilder.append(pair.key()).append("\t").append(pair.value()).append("\n");
        }

        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);
        s3ClientService.writeData(payload.bucketName(), finalPath, outputBuilder.toString());

        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: Output saved to {} ] ---", finalPath);
    }
}