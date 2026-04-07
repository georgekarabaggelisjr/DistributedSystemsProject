package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
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

        // Step 1: Download User Code
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + payload.className() + ".class";
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // Step 2: Dynamically load the Mapper
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
     * Orchestrates the complete lifecycle of a REDUCE task.
     *
     * @param payload The structured data containing all routing and execution metadata.
     * @throws Exception If any operational boundary fails.
     */
    private void executeReducePhase(TaskPayload payload) throws Exception {
        logger.info("--- [ STARTING REDUCE PHASE: Task {} ] ---", payload.taskId());

        // TODO: Implement Reduce orchestration (Download intermediate files, group by key, run ReduceTaskProcessor)
        logger.warn("Reduce phase orchestration is not yet fully implemented in TaskExecutor.");

        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: Task {} ] ---", payload.taskId());
    }
}