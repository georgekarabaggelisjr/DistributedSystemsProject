package com.iliasbolan.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.engine.TaskExecutor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the connection to the Message Broker (RabbitMQ) and orchestrates task consumption.
 * <p>
 * This class serves as the primary lifecycle controller for the Worker node. It implements
 * an <b>Enterprise 2-Queue Architecture</b>:
 * </p>
 * <ul>
 * <li><b>Work Queue (Ingress):</b> Consumes task assignments (Map/Reduce chunks).</li>
 * <li><b>Status Queue (Egress):</b> Publishes real-time state transitions back to the Manager
 * node (COMPLETED/FAILED).</li>
 * </ul>
 * <p>
 * <b>Observability:</b> Utilizes SLF4J's Mapped Diagnostic Context (MDC) to ensure all logs
 * are correlated with specific Job and Task IDs, mirroring the Manager's logging patterns.
 * </p>
 * <p>
 * <b>Elasticity:</b> Features an integrated Idle Checker that monitors message throughput.
 * If no messages are received within the {@code idleTimeoutMillis} threshold, the consumer
 * initiates a self-termination sequence to support "Scale-to-Zero" infrastructure.
 * </p>
 * <p>
 * <b>Poison Pill Protection:</b> Implements a Max Retry Limit. If a specific data chunk causes
 * repeated JVM failures or logic errors, the consumer will eventually discard the message
 * after {@code MAX_RETRIES} to prevent infinite loops and resource exhaustion.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.3
 * @see com.iliasbolan.engine.TaskExecutor
 * @since 2026-04-07
 */
public class RabbitMqConsumer {

    /** Logger instance for distributed event tracking. */
    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);

    /** High-performance JSON mapper for metadata extraction. */
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /** Maximum number of times a task can be requeued before being discarded. */
    private static final int MAX_RETRIES = 3;

    /** Manages the underlying TCP connection to the RabbitMQ cluster. */
    private final RabbitMqConnectionManager connectionManager;

    /** The name of the primary queue containing Map/Reduce task payloads. */
    private final String queueName;

    /** Maximum allowable duration in milliseconds to remain idle before self-termination. */
    private final int idleTimeoutMillis;

    /** The execution engine where consumed tasks are dispatched. */
    private final TaskExecutor taskExecutor;

    /** The designated queue for broadcasting orchestration feedback to the Manager. */
    private final String statusQueueName = "job_events_queue";

    /** Reference to the main thread to facilitate graceful interrupts. */
    private Thread mainThread;

    /**
     * Constructs a new {@code RabbitMqConsumer} with the specified connection parameters and execution engine.
     *
     * @param connectionManager Factory for creating RabbitMQ connections.
     * @param queueName Target queue for incoming tasks.
     * @param idleTimeoutMillis Threshold for inactivity-based shutdown.
     * @param taskExecutor The computation engine for processing tasks.
     */
    public RabbitMqConsumer(RabbitMqConnectionManager connectionManager, String queueName, int idleTimeoutMillis, TaskExecutor taskExecutor) {
        this.connectionManager = connectionManager;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.taskExecutor = taskExecutor;

        logger.info("Initialized RabbitMqConsumer. Target Queue: {}, Status Queue: {}, Idle Timeout: {}ms",
                queueName, statusQueueName, idleTimeoutMillis);
    }

    /**
     * Initiates a clean shutdown of the consumer by interrupting the main processing thread.
     */
    public void stopConsuming() {
        if (this.mainThread != null) {
            logger.info("Interrupting main consumer thread to initiate clean shutdown...");
            this.mainThread.interrupt();
        }
    }

    /**
     * Establishes the message broker connection and enters the primary consumption loop.
     * <p>
     * This method:
     * <ol>
     * <li>Declares necessary queue topology.</li>
     * <li>Configures Quality of Service (QoS) to prevent worker over-subscription.</li>
     * <li>Registers an asynchronous callback for incoming deliveries.</li>
     * <li>Launches a daemon thread to monitor for idle-based termination.</li>
     * </ol>
     * </p>
     *
     * @throws Exception If the broker is unreachable or queue declaration fails.
     */
    public void startConsuming() throws Exception {
        this.mainThread = Thread.currentThread();

        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            // Ensure the messaging fabric is durable and ready
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueDeclare(statusQueueName, true, false, false, null);

            // CRITICAL: Prefetch(1) ensures load balancing is fair and pods don't hoard messages
            channel.basicQos(1);

            logger.info("Successfully connected to RabbitMQ. Waiting for messages on queue: '{}'.", queueName);

            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    lastActivity.set(System.currentTimeMillis());
                    String messageBody = new String(body, StandardCharsets.UTF_8);
                    long deliveryTag = envelope.getDeliveryTag();

                    // 1. Determine current delivery attempt (supports Quorum Queues x-delivery-count)
                    long deliveryCount = getDeliveryCount(properties);

                    // Extract correlation metadata for logging and status reporting
                    String jobId = "UNKNOWN";
                    String taskId = "UNKNOWN";
                    String phase = "UNKNOWN";
                    try {
                        JsonNode jsonNode = objectMapper.readTree(messageBody);
                        jobId = jsonNode.path("jobId").asText("UNKNOWN");
                        taskId = jsonNode.path("taskId").asText("UNKNOWN");
                        phase = jsonNode.path("taskType").asText("UNKNOWN");
                    } catch (Exception e) {
                        logger.warn("Could not parse JSON payload to extract Job/Task IDs for status reporting.", e);
                    }

                    try {
                        // Apply MDC tags for log correlation (EFK/Loki/Splunk compatible)
                        MDC.put("job_id", jobId);
                        MDC.put("task_id", taskId);
                        MDC.put("phase", phase);
                        MDC.put("delivery_tag", String.valueOf(deliveryTag));
                        MDC.put("retry_count", String.valueOf(deliveryCount));

                        // 2. CHECK FOR POISON PILL: If we exceeded max retries, fail terminal
                        if (deliveryCount > MAX_RETRIES) {
                            logger.error("Poison Pill Detected! Task exceeded MAX_RETRIES ({}). Discarding message.", MAX_RETRIES);

                            String failedEvent = String.format("{\"jobId\": \"%s\", \"taskId\": \"%s\", \"state\": \"FAILED\", \"error\": \"MAX_RETRIES_EXCEEDED\"}", jobId, taskId);
                            channel.basicPublish("", statusQueueName, null, failedEvent.getBytes(StandardCharsets.UTF_8));

                            // NACK with requeue=false to remove it from the fabric
                            channel.basicNack(deliveryTag, false, false);
                            return;
                        }

                        logger.info("Received Task Payload (Attempt {}/{}): {}", deliveryCount, MAX_RETRIES, messageBody);

                        try {
                            // Dispatch task to the execution engine
                            taskExecutor.executeTask(messageBody);

                            // Notify the Manager of a successful completion
                            String successEvent = String.format("{\"jobId\": \"%s\", \"taskId\": \"%s\", \"state\": \"COMPLETED\"}", jobId, taskId);
                            channel.basicPublish("", statusQueueName, null, successEvent.getBytes(StandardCharsets.UTF_8));

                            // Acknowledge the message only after successful persistence of results
                            channel.basicAck(deliveryTag, false);
                            logger.info("Task successfully processed, event broadcasted, and ACK sent.");

                        } catch (Throwable t) {
                            logger.error("Critical error processing task. Sending FAILED event and NACKing for requeue.", t);

                            // Notify the Manager of the failure to trigger remediation
                            String failedEvent = String.format("{\"jobId\": \"%s\", \"taskId\": \"%s\", \"state\": \"FAILED\", \"error\": \"%s\"}",
                                    jobId, taskId, t.getClass().getSimpleName());
                            channel.basicPublish("", statusQueueName, null, failedEvent.getBytes(StandardCharsets.UTF_8));

                            // Negative Acknowledgment (NACK) with requeue=true to allow another pod to try
                            channel.basicNack(deliveryTag, false, true);
                        }
                    } finally {
                        // CRITICAL: Clear MDC context to prevent data bleeding between tasks
                        MDC.clear();
                    }
                }
            };

            // Register the consumer with manual acknowledgments enabled
            channel.basicConsume(queueName, false, consumer);

            // Startup the idle watcher daemon
            Thread idleChecker = getThread(lastActivity);
            idleChecker.start();

            try {
                // Keep the main thread alive until interrupted
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                logger.warn("Main consumer thread interrupted. Releasing RabbitMQ resources...", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Extracts the delivery count from RabbitMQ headers.
     * * @param properties The message properties containing headers.
     * @return The current delivery count, defaults to 1 if not present.
     */
    private long getDeliveryCount(AMQP.BasicProperties properties) {
        Map<String, Object> headers = properties.getHeaders();
        if (headers != null && headers.containsKey("x-delivery-count")) {
            return ((Number) headers.get("x-delivery-count")).longValue();
        }
        return 1;
    }

    /**
     * Creates a daemon thread that monitors inactivity and triggers shutdown.
     *
     * @param lastActivity Atomic timestamp of the last message delivery.
     * @return A configured daemon Thread.
     */
    @NotNull
    private Thread getThread(AtomicLong lastActivity) {
        Thread idleChecker = new Thread(() -> {
            try {
                while (true) {
                    long elapsed = System.currentTimeMillis() - lastActivity.get();
                    long remainingToWait = idleTimeoutMillis - elapsed;

                    if (remainingToWait <= 0) {
                        logger.info("No messages received for {} seconds. Gracefully terminating worker phase.", (idleTimeoutMillis / 1000));
                        stopConsuming();
                        break;
                    } else {
                        // Adaptive sleep to reduce CPU polling overhead
                        Thread.sleep(remainingToWait);
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("Idle checker thread interrupted.", e);
                Thread.currentThread().interrupt();
            }
        });

        idleChecker.setDaemon(true); // Ensure this thread doesn't prevent JVM shutdown
        return idleChecker;
    }
}