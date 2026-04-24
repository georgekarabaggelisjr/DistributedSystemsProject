package com.iliasbolan.core;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * A Data Transfer Object (DTO) representing the JSON payload sent by the Manager Service.
 * <p>
 * <b>Architectural Role:</b><br>
 * In the distributed Map-Reduce architecture, the Manager Service produces task assignments
 * as JSON messages and pushes them to the RabbitMQ broker. This <code>record</code> acts as the
 * strict Java representation of that JSON contract, allowing the Worker node to deserialize
 * routing and execution metadata safely. With the transition to a Peer-to-Peer (P2P)
 * Shuffle architecture, this payload now also acts as a dynamic directory, carrying the
 * routing table required for direct worker-to-worker data transfers.
 * </p>
 * <p>
 * <b>Immutability & Thread-Safety:</b><br>
 * By utilizing a Java 17 <code>record</code>, this payload is inherently immutable. Once instantiated
 * by the Jackson parser, the configuration variables are locked. This guarantees thread safety
 * when passing the payload across the parallel boundaries of the Fork/Join execution pool.
 * </p>
 * <p>
 * <b>Forward Compatibility:</b><br>
 * Annotated with <code>@JsonIgnoreProperties(ignoreUnknown = true)</code> to ensure that if the
 * Manager Service evolves to send additional metadata fields, older worker nodes will safely
 * ignore them rather than throwing an <code>UnrecognizedPropertyException</code>.
 * </p>
 *
 * @param jobId          The unique identifier of the overarching Map-Reduce job.
 * @param taskId         The unique identifier for this specific chunk/partition.
 * @param taskType       The operational phase to execute. Expected values: <code>"MAP"</code> or <code>"REDUCE"</code>.
 * @param bucketName     The MinIO bucket where the input data (or intermediate data) resides.
 * @param objectName     The exact S3 object key for the input data.
 * @param byteOffset     For MAP tasks: The starting byte position to read the 64MB chunk from.
 * @param byteLength     For MAP tasks: The total number of bytes to read (e.g., <code>67108864</code>).
 * @param numReducers    The total number of reducers (R) configured for the job, used in the Shuffle phase.
 * @param userCodeBucket The MinIO bucket where the user's compiled <code>.class</code> or <code>.jar</code> file lives.
 * @param userCodeObject The MinIO object key for the user's compiled execution code.
 * @param className      The fully qualified name of the user's Mapper or Reducer class to be loaded via Reflection.
 * @param workerEndpoints A routing table of network addresses (IP:Port) for active Map workers. Used during the REDUCE phase to pull intermediate partitions directly (P2P), bypassing MinIO/S3.
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-07
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TaskPayload(
        String jobId,
        String taskId,
        String taskType,
        String bucketName,
        String objectName,
        long byteOffset,
        long byteLength,
        int numReducers,
        String userCodeBucket,
        String userCodeObject,
        String className,
        List<String> workerEndpoints
) {}