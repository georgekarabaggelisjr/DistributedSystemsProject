package com.iliasbolan.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles the transmission of task lifecycle events from the Worker to the Manager.
 * <p>
 * In a distributed MapReduce architecture, the Manager must maintain an accurate
 * state of the cluster. This producer is responsible for signaling when a task
 * has transitioned to a 'COMPLETED' state and providing the network coordinates
 * (gRPC address) where the intermediate shuffle data is hosted.
 * </p>
 * <p>
 * <b>Schema Synchronization:</b><br>
 * The outgoing JSON messages are strictly formatted to match the Pydantic
 * <code>TaskCompletedRequest</code> model used by the Python-based Orchestrator,
 * ensuring seamless cross-language communication via RabbitMQ.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.0
 * @since 2026-04-23
 * @see com.iliasbolan.messaging.RabbitMqConnectionManager
 */
public class RabbitMqProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqProducer.class);

    /** High-performance JSON serializer for event payloads. */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final RabbitMqConnectionManager connectionManager;
    private final String eventQueue;

    /**
     * Constructs a new producer for signaling task completion events.
     *
     * @param connectionManager The shared manager providing authenticated RabbitMQ connections.
     * @param eventQueue        The name of the queue where the Manager listens for events (e.g., "job_events_queue").
     */
    public RabbitMqProducer(RabbitMqConnectionManager connectionManager, String eventQueue) {
        this.connectionManager = connectionManager;
        this.eventQueue = eventQueue;

        logger.info("Initialized RabbitMqProducer. Target Event Queue: {}", eventQueue);
    }

    /**
     * Publishes a task completion signal to the Manager.
     * <p>
     * This method encapsulates the network address of the worker's gRPC server,
     * allowing the Manager to update the global routing table so that subsequent
     * Reduce tasks can find their required data partitions.
     * </p>
     *
     * @param jobId             The unique UUID of the MapReduce job.
     * @param taskId            The specific identifier of the completed Map task.
     * @param status            The final execution status (typically "COMPLETED").
     * @param workerBindAddress The gRPC network endpoint (e.g., "10.244.1.5:50051")
     * where Reducers can stream shuffle data.
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
}