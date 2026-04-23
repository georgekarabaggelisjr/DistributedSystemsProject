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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
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
 * <li><b>gRPC P2P Shuffle:</b> Utilizing gRPC Server-Side Streaming to fetch intermediate data.</li>
 * <li><b>Spill-to-Disk Resilience:</b> Streams gRPC payloads and Map results directly to the local filesystem to prevent OOM errors on massive datasets.</li>
 * <li><b>Lifecycle Signaling:</b> Reporting task completion and network coordinates to the Manager via {@link RabbitMqProducer}.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 4.1
 * @since 2026-04-24
 */
public class TaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(TaskExecutor.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final S3ClientService s3ClientService;
    private final ForkJoinPool forkJoinPool;
    private final String baseShuffleDir;
    private final RabbitMqProducer eventProducer;
    private final String podIp;

    public TaskExecutor(S3ClientService s3ClientService, String baseShuffleDir, RabbitMqProducer eventProducer, String podIp) {
        this.s3ClientService = s3ClientService;
        this.baseShuffleDir = baseShuffleDir;
        this.eventProducer = eventProducer;
        this.podIp = podIp;
        this.forkJoinPool = ForkJoinPool.commonPool();

        logger.info("Initialized TaskExecutor with parallel ForkJoinPool (Parallelism: {}) and Pod IP: {}",
                forkJoinPool.getParallelism(), podIp);
    }

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
     * """
     * Executes the Map phase utilizing a strictly memory-safe disk-spill architecture.
     * * Downloads the required data chunk and delegates computation to the ForkJoinPool.
     * The processing threads stream intermediate pairs directly to the local disk via
     * the ShufflePartitioner, completely bypassing memory-bound lists.
     * * Args:
     * payload (TaskPayload): The metadata required to execute the specific map chunk.
     * * Raises:
     * Throwable: If an error occurs during resource acquisition or data processing.
     * """
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

        // Initialize the partitioner first to handle concurrent disk spills
        ShufflePartitioner partitioner = new ShufflePartitioner(
                baseShuffleDir,
                payload.jobId(),
                payload.taskId(),
                payload.numReducers()
        );

        // Pass the partitioner into the MapTaskProcessor
        MapTaskProcessor rootMapTask = new MapTaskProcessor(records, 0, records.size(), mapper, partitioner);

        // Execute the void action. Threads will process batches and flush to disk independently.
        forkJoinPool.invoke(rootMapTask);

        logger.info("Parallel Map processing and disk-spilling complete.");

        String grpcPort = System.getenv().getOrDefault("SHUFFLE_GRPC_PORT", "50051");
        String workerBindAddress = this.podIp + ":" + grpcPort;

        eventProducer.sendCompletionSignal(payload.jobId(), payload.taskId(), "COMPLETED", workerBindAddress);

        logger.info("--- [ SUCCESSFULLY COMPLETED MAP PHASE: Task {} ] ---", payload.taskId());
    }

    /**
     * Orchestrates the REDUCE task lifecycle utilizing gRPC Server-Side Streaming and Spill-to-Disk.
     *
     * """
     * Executes the Reduce phase by streaming remote partition data directly to local disk.
     * * This implements the first stage of the Spill-to-Disk architecture, preventing OOM
     * errors by bypassing in-memory collections for the raw gRPC payload. It then delegates
     * to the ExternalMergeSorter for grouped processing.
     * * Args:
     * payload (TaskPayload): The metadata required to aggregate the intermediate partition.
     * * Raises:
     * Throwable: If gRPC network fetching, sorting, or final output persistence fails.
     * """
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

                // Consume the gRPC stream and write bytes directly to disk (UTF-8 safe)
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

        // 3. External Merge Sort and Reduce
        logger.info("gRPC streams persisted. Initiating External Merge Sort...");

        // Output path for final S3 upload
        String finalPath = String.format("%s/output/result_part_%d.txt", payload.jobId(), partitionIndex);

        // The ExternalMergeSorter handles chunking, sorting, and passing data to the Reducer
        // It returns the file path containing the final reduced results
        Path finalReducedFile = ExternalMergeSorter.sortReduceAndSpill(rawDataDir, reducer, forkJoinPool);

        // 4. Persist final results to S3
        logger.info("Uploading final reduced data to S3...");
        s3ClientService.writeData(payload.bucketName(), finalPath, Files.readString(finalReducedFile));

        // Cleanup local temporary files to preserve disk space
        ExternalMergeSorter.cleanupDirectory(rawDataDir);

        logger.info("--- [ SUCCESSFULLY COMPLETED REDUCE PHASE: {} ] ---", finalPath);
    }
}