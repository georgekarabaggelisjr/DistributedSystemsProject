package com.iliasbolan.engine;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.core.KeyValuePair;
import com.iliasbolan.core.Mapper;
import com.iliasbolan.core.Reducer;
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

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

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
 * <li><b>gRPC P2P Shuffle:</b> Utilizing gRPC Server-Side Streaming to fetch intermediate data directly from sibling pods.</li>
 * <li><b>Lifecycle Signaling:</b> Reporting task completion and network coordinates to the Manager via {@link RabbitMqProducer}.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 3.0
 * @since 2026-04-07
 */
public class TaskExecutor {

    /** Logger instance for tracking task execution and system health. */
    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    /** * High-performance JSON mapper for processing RabbitMQ message bodies.
     * Configured to ignore unknown properties to ensure compatibility with evolving Manager schemas.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Service for S3-compatible storage interactions (Used for Code/Final Output). */
    private final S3ClientService s3ClientService;

    /** Shared pool for executing parallelized Map and Reduce operations. */
    private final ForkJoinPool forkJoinPool;

    /** The root directory for local P2P shuffle data persistence. */
    private final String baseShuffleDir;

    /** The producer responsible for signaling task events back to the Manager. */
    private final RabbitMqProducer eventProducer;

    /** The network identity of this pod, used for gRPC callback registration. */
    private final String podIp;

    /**
     * Constructs a new {@code TaskExecutor} with a shared parallel execution pool and identity context.
     *
     * @param s3ClientService The initialized service for S3 storage I/O.
     * @param baseShuffleDir  The local disk directory for hosting P2P intermediate data.
     * @param eventProducer   The producer used to signal completion events back to the Orchestrator.
     * @param podIp           The internal Kubernetes IP of this pod, used to build gRPC endpoints.
     */
    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String podIp) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.podIp = podIp;
        this.forkJoinPool = ForkJoinPool.commonPool();

        logger.info("Initialized TaskExecutor with parallel ForkJoinPool (Parallelism: {}) and Pod IP: {}",
                forkJoinPool.getParallelism(), podIp);
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
     * <p>
     * <b>Coordination Logic:</b><br>
     * Upon successful completion of the map operation and local partitioning, this method
     * resolves the worker's network identity and broadcasts it along with the
     * gRPC service port to the Manager.
     * </p>
     *
     * @param payload The metadata required to execute the specific map chunk.
     * @throws Throwable If an error occurs during resource acquisition or data processing.
     */
    private void executeMapPhase(TaskPayload payload) throws Throwable {
        logger.info("--- [ STARTING MAP PHASE: Task {} ] ---", payload.taskId());

        // 1. Localize user-provided Mapper code
        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        // 2. Instantiate logic via Dynamic Class Loading
        Mapper mapper = DynamicClassLoader.loadMapper(localCodeDir, payload.className());

        // 3. Stream data chunk from S3/MinIO
        List<String> records = s3ClientService.readDataChunk(
                payload.bucketName(),
                payload.objectName(),
                payload.byteOffset(),
                payload.byteLength()
        );

        logger.info("Successfully received {} clean records from S3 service.", records.size());

        // 4. Parallelize execution using the MapTaskProcessor (ForkJoin)
        MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper);
        List<KeyValuePair> intermediateResults = forkJoinPool.invoke(rootMapTask);

        logger.info("Parallel Map processing complete. Generated {} intermediate pairs.", intermediateResults.size());

        // 5. Partition results to local disk for gRPC Shuffle Service
        ShufflePartitioner partitioner = new ShufflePartitioner(
                baseShuffleDir,
                payload.jobId(),
                payload.taskId(),
                payload.numReducers()
        );
        partitioner.partitionAndWriteLocal(intermediateResults);

        // 6. Signal completion and report gRPC coordinates (PodIP:Port) to Manager
        String grpcPort = System.getenv().getOrDefault("SHUFFLE_GRPC_PORT", "50051");
        String workerBindAddress = this.podIp + ":" + grpcPort;

        eventProducer.sendCompletionSignal(payload.jobId(), payload.taskId(), "COMPLETED", workerBindAddress);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", payload.taskId());
    }

    /**
     * Orchestrates the REDUCE task lifecycle utilizing gRPC Server-Side Streaming for P2P data transfer.
     * <p>
     * <b>gRPC Shuffle Protocol:</b><br>
     * Establish persistent {@link ManagedChannel} connections to sibling nodes to fetch
     * partition streams, significantly reducing serialization overhead.
     * </p>
     *
     * @param payload The metadata required to aggregate the specific intermediate partition.
     * @throws Throwable If gRPC network fetching, aggregation, or final output persistence fails.
     */
    private void executeReducePhase(TaskPayload payload) throws Throwable {
        logger.info("--- [ STARTING REDUCE PHASE: Partition {} for Job {} ] ---", payload.taskId(), payload.jobId());

        // 1. Localize Reducer code
        String packagePath = payload.className().replace(".", "/");
        String localCodeDir = "/tmp/mapreduce/usercode/" + payload.jobId() + "/";
        String localCodePath = localCodeDir + packagePath + ".class";

        java.io.File fileObj = new java.io.File(localCodePath);
        fileObj.getParentFile().mkdirs();
        s3ClientService.downloadUserCode(payload.userCodeBucket(), payload.userCodeObject(), localCodePath);

        Reducer reducer = DynamicClassLoader.loadReducer(localCodeDir, payload.className());

        int partitionIndex = Integer.parseInt(payload.taskId());
        java.util.Map<String, List<String>> groupedData = new java.util.HashMap<>();

        // 2. Fetch data from Map workers via gRPC
        List<String> endpoints = payload.workerEndpoints();
        logger.info("Initiating gRPC P2P data transfer. Streaming from {} Map workers...", endpoints.size());

        for (String endpoint : endpoints) {
            String[] addressParts = endpoint.split(":");
            String host = addressParts[0];
            int port = (addressParts.length > 1) ? Integer.parseInt(addressParts[1]) : 50051;

            ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext() // Internal cluster traffic bypasses TLS for performance
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .build();

            try {
                ShuffleServiceGrpc.ShuffleServiceBlockingStub stub = ShuffleServiceGrpc.newBlockingStub(channel);
                PartitionRequest request = PartitionRequest.newBuilder()
                        .setJobId(payload.jobId())
                        .setPartitionId(partitionIndex)
                        .build();

                // Consume the gRPC stream
                Iterator<PartitionChunk> chunkStream = stub.getPartition(request);
                while (chunkStream.hasNext()) {
                    PartitionChunk chunk = chunkStream.next();
                    String content = chunk.getContent().toStringUtf8();

                    for (String line : content.split("\\n")) {
                        if (line.isBlank()) continue;
                        String[] parts = line.split("\\t");
                        if (parts.length == 2) {
                            groupedData.computeIfAbsent(parts[0], k -> new ArrayList<>()).add(parts[1]);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("gRPC P2P Error: Failed to stream from node {}", endpoint, e);
                throw new RuntimeException("Critical gRPC shuffle failure.", e);
            } finally {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        // 3. Sort and Reduce the grouped data
        logger.info("Sorting {} unique keys for partition {}...", groupedData.size(), partitionIndex);
        List<java.util.Map.Entry<String, List<String>>> sortedEntries = new ArrayList<>(groupedData.entrySet());
        sortedEntries.sort(java.util.Map.Entry.comparingByKey());

        ReduceTaskProcessor rootReduceTask = new ReduceTaskProcessor(sortedEntries, 0, sortedEntries.size(), reducer);
        List<KeyValuePair> finalResults = forkJoinPool.invoke(rootReduceTask);

        // 4. Serialize and persist final part file to S3
        StringBuilder outputBuilder = new StringBuilder();
        for (KeyValuePair pair : finalResults) {
            outputBuilder.append(pair.key()).append("\t").append(pair.value()).append("\n");
        }

        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);
        s3ClientService.writeData(payload.bucketName(), finalPath, outputBuilder.toString());

        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: {} ] ---", finalPath);
    }
}