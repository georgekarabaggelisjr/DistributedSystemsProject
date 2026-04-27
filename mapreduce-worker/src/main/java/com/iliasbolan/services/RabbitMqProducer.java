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
 * Handles the transmission of task lifecycle events from the Worker node to the global Orchestrator.
 * <p>
 * This producer serves as the primary feedback loop for the distributed system, allowing
 * workers to report task state transitions. It utilizes a short-lived connection pattern
 * to ensure that signaling does not block computational resources.
 * </p>
 * <p>
 * <b>Schema Synchronization & Security:</b><br>
 * Outgoing JSON payloads are strictly structured to align with the Pydantic
 * models used by the Python-based Manager API. This class is responsible for
 * echoing the secure HMAC <code>jobToken</code> back to the Manager, providing
 * origin authentication for every completion and error signal.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-25
 * @see com.iliasbolan.infrastructure.RabbitMqConnectionManager
 */
public class RabbitMqProducer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqProducer.class);

    /**
     * Shared Jackson mapper for serializing event payloads into standard JSON format.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final RabbitMqConnectionManager connectionManager;
    private final String eventQueue;

    /**
     * Constructs a new RabbitMqProducer for worker-to-manager signaling.
     *
     * @param connectionManager The infrastructure manager responsible for AMQP connection pooling.
     * @param eventQueue        The destination queue name where the Orchestrator listens for task updates.
     */
    public RabbitMqProducer(RabbitMqConnectionManager connectionManager, String eventQueue) {
        this.connectionManager = connectionManager;
        this.eventQueue = eventQueue;
    }

    /**
     * Transmits a successful task completion signal to the global Orchestrator.
     * <p>
     * This signal includes the <code>workerBindAddress</code>, which the Manager
     * uses to populate the Worker Registry. This is a critical prerequisite for
     * the <b>Data Locality Scheduler</b> to effectively pin Reducer pods to
     * nodes holding the intermediate shuffle data.
     * </p>
     *
     * @param jobId             The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId            The specific partition or chunk identifier of the completed task.
     * @param jobToken          The secure token echoed back to the Manager for signal authentication.
     * @param status            The execution status, typically "COMPLETED".
     * @param workerBindAddress The gRPC network endpoint (e.g., "node@@10.244.1.5:7337") for P2P shuffle fetches.
     */
    public void sendCompletionSignal(String jobId, String taskId, String jobToken, String status, String workerBindAddress) {
        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("jobToken", jobToken); // Injected Secure Token for Manager verification
            payload.put("status", status);
            payload.put("workerBindAddress", workerBindAddress);

            byte[] messageBody = objectMapper.writeValueAsBytes(payload);
            channel.basicPublish("", eventQueue, null, messageBody);

            logger.info("Successfully signaled task completion to Manager. Job: {}, Task: {}, gRPC Endpoint: {}",
                    jobId, taskId, workerBindAddress);

        } catch (Exception e) {
            logger.error("Critical Failure: Could not transmit completion signal for task {} to the Manager.", taskId, e);
        }
    }

    /**
     * Transmits a targeted task failure event to the global Orchestrator.
     * <p>
     * Targeted failure reporting is essential for <b>Reactive Lineage Recovery</b>.
     * By providing a specific error string (e.g., indicating a shuffle fetch failure),
     * the Manager can initiate a surgical re-run of specific upstream dependencies
     * rather than failing the entire distributed job.
     * </p>
     *
     * @param jobId    The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId   The specific partition identifier of the task that encountered the failure.
     * @param jobToken The secure token echoed back to the Manager for signal authentication.
     * @param status   The execution status, typically "FAILED".
     * @param error    The specific error diagnostic string for lineage recomputation.
     */
    public void sendErrorSignal(String jobId, String taskId, String jobToken, String status, String error) {
        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("jobToken", jobToken); // Injected Secure Token for Manager verification
            payload.put("status", status);
            payload.put("error", error);

            byte[] messageBody = objectMapper.writeValueAsBytes(payload);
            channel.basicPublish("", eventQueue, null, messageBody);

            logger.warn("Transmitted targeted error signal to Manager. Job: {}, Task: {}, Error: {}",
                    jobId, taskId, error);

        } catch (Exception e) {
            logger.error("Critical Failure: Could not transmit error signal for task {} to the Manager.", taskId, e);
        }
    }

    /**
     * Transmits an 'IN_PROGRESS' heartbeat signal to the global Orchestrator.
     * <p>
     * This continuous heartbeat prevents the Orchestrator's Watchdog service from
     * prematurely terminating long-running tasks that process massive datasets.
     * </p>
     *
     * @param jobId    The universally unique identifier (UUID) of the Map-Reduce job.
     * @param taskId   The specific partition identifier of the task in progress.
     * @param jobToken The secure token echoed back to the Manager for signal authentication.
     */
    public void sendProgressSignal(String jobId, String taskId, String jobToken) {
        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            Map<String, Object> payload = new HashMap<>();
            payload.put("job_id", jobId);
            payload.put("task_id", taskId);
            payload.put("jobToken", jobToken); // Injected Secure Token for Manager verification
            payload.put("status", "IN_PROGRESS");

            byte[] messageBody = objectMapper.writeValueAsBytes(payload);
            channel.basicPublish("", eventQueue, null, messageBody);

            logger.debug("Transmitted IN_PROGRESS heartbeat to Manager. Job: {}, Task: {}", jobId, taskId);

        } catch (Exception e) {
            logger.warn("Failed to transmit heartbeat signal for task {}. Watchdog may intervene if prolonged.", taskId, e);
        }
    }
}