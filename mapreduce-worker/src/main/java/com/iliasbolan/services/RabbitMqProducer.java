package com.iliasbolan.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles the transmission of task lifecycle events from the Worker node to the global Manager.
 *
 * <p>In a distributed MapReduce architecture, the Manager must maintain an accurate
 * and synchronized state of the cluster. This producer is responsible for signaling
 * state transitions—specifically when a task reaches a {@code COMPLETED} or
 * {@code FAILED} state—and providing the necessary network coordinates (gRPC
 * addresses) required for subsequent shuffle phases.</p>
 *
 * <p><b>Schema Synchronization:</b><br>
 * Outgoing JSON payloads are strictly structured to align with the Pydantic
 * {@code TaskCompletedRequest} model utilized by the Python-based Orchestrator.
 * This ensures deterministic cross-language communication and type safety
 * across the RabbitMQ message broker.</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-23
 * @see RabbitMqConnectionManager
 */
public class RabbitMqProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqProducer.class);

    /** High-performance JSON serializer for event payloads. */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** The manager responsible for providing authenticated RabbitMQ connections. */
    private final RabbitMqConnectionManager connectionManager;

    /** The target routing key or queue name for task event signals. */
    private final String eventQueue;

    /**
     * Constructs a new {@code RabbitMqProducer} with a specific connection manager
     * and target queue.
     *
     * @param connectionManager The manager for acquiring live RabbitMQ connections.
     * @param eventQueue        The name of the queue where task events are published.
     */
    public RabbitMqProducer(RabbitMqConnectionManager connectionManager, String eventQueue) {
        this.connectionManager = connectionManager;
        this.eventQueue = eventQueue;
    }

    /**
     * Transmits a successful task completion signal to the global Orchestrator.
     *
     * <p>This method encapsulates the payload construction and serialization logic
     * required to notify the control plane that a specific chunk has been processed
     * and its intermediate shuffle data is ready for retrieval.</p>
     *
     * @param jobId             The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId            The specific partition or chunk identifier of the completed task.
     * @param status            The execution status, typically "COMPLETED".
     * @param workerBindAddress The gRPC network endpoint (e.g., "10.244.1.5:50051")
     * where sibling nodes can stream the generated shuffle data.
     */
    public void sendCompletionSignal(String jobId, String taskId, String status, String workerBindAddress) {
        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            // Step 1: Construct the payload matching the Manager's Pydantic schema
            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("status", status);
            payload.put("worker_bind_address", workerBindAddress);

            // Step 2: Serialize the payload to a JSON byte array
            byte[] messageBody = objectMapper.writeValueAsBytes(payload);

            // Step 3: Publish to the default exchange with the event queue as the routing key
            channel.basicPublish("", eventQueue, null, messageBody);

            logger.info("Successfully signaled task completion to Manager. Job: {}, Task: {}, gRPC Endpoint: {}",
                    jobId, taskId, workerBindAddress);

        } catch (Exception e) {
            logger.error("Critical Failure: Could not transmit completion signal for task {} to the Manager.", taskId, e);
        }
    }

    /**
     * Transmits a targeted task failure event to the global Orchestrator.
     *
     * <p><b>Reactive Lineage Recomputation:</b><br>
     * This method is utilized by the <i>Sentinel Interceptor</i> during the Peer-to-Peer
     * Shuffle phase. If a worker detects a remote pod has been evicted or crashed
     * (ephemeral data loss), it dispatches a specialized {@code SHUFFLE_FETCH_FAILED}
     * error payload. This bypasses the global Fail-Fast teardown and triggers
     * the Manager's self-healing lineage recovery.</p>
     *
     * @param jobId  The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId The specific partition identifier of the task that encountered the failure.
     * @param status The execution status, typically "FAILED".
     * @param error  The specific error diagnostic string (e.g., "SHUFFLE_FETCH_FAILED:map-chunk-5").
     */
    public void sendErrorSignal(String jobId, String taskId, String status, String error) {
        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("status", status);
            payload.put("error", error); // Injects the error string for orchestrator-side parsing

            byte[] messageBody = objectMapper.writeValueAsBytes(payload);

            channel.basicPublish("", eventQueue, null, messageBody);

            logger.warn("Transmitted targeted error signal to Manager. Job: {}, Task: {}, Error: {}",
                    jobId, taskId, error);

        } catch (Exception e) {
            logger.error("Critical Failure: Could not transmit error signal for task {} to the Manager.", taskId, e);
        }
    }
}