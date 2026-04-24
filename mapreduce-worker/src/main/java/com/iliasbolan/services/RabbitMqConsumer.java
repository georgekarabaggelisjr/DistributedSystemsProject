package com.iliasbolan.services;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.CancelCallback;
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
 * Orchestrates the lifecycle of task consumption from the RabbitMQ message broker.
 * <p>
 * This class serves as the primary event-driven controller for the Worker node, implementing
 * essential distributed systems patterns to ensure reliability in containerized environments.
 * </p>
 * <h3>Key Distributed Patterns:</h3>
 * <ul>
 * <li><b>Scale-to-Zero:</b> Implements an adaptive idle-monitoring daemon that triggers
 * graceful shutdown when the task queue remains empty, allowing Kubernetes to reclaim resources.</li>
 * <li><b>Fault Tolerance (Poison Pill):</b> Interrogates message headers for delivery counts,
 * automatically discarding tasks that cause recurring crashes to prevent infinite failure loops.</li>
 * <li><b>Dynamic Schema Resilience:</b> Leverages a lenient JSON mapper to remain compatible
 * with new metadata introduced by the Manager during system upgrades (e.g., gRPC endpoint tables).</li>
 * <li><b>Contextual Observability:</b> Utilizes Mapped Diagnostic Context (MDC) to ensure
 * all log entries are correlated with specific Job and Task IDs.</li>
 * </ul>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-04-07
 */
public class RabbitMqConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);

    /**
     *  Configured JSON mapper for task extraction.
     * Set to ignore unknown properties to facilitate seamless communication with
     * the Python Manager's Pydantic-based schemas.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Limit for delivery attempts before a message is rejected as a "Poison Pill". */
    private static final int MAX_RETRIES = 3;

    private final RabbitMqConnectionManager connectionManager;
    private final String queueName;
    private final int idleTimeoutMillis;
    private final TaskExecutor taskExecutor;
    private final RabbitMqProducer eventProducer;
    private Thread mainThread;

    /**
     * Constructs a consumer with integrated task execution and event signaling capabilities.
     *
     * @param connectionManager Factory for acquiring authenticated TCP connections.
     * @param queueName         The AMQP queue to monitor for workload definitions.
     * @param idleTimeoutMillis The duration of inactivity permitted before self-termination.
     * @param taskExecutor      The engine responsible for the physical execution of Map/Reduce logic.
     * @param eventProducer     The utility for broadcasting execution state changes to the Manager.
     */
    public RabbitMqConsumer(RabbitMqConnectionManager connectionManager, String queueName,
                            int idleTimeoutMillis, TaskExecutor taskExecutor, RabbitMqProducer eventProducer) {
        this.connectionManager = connectionManager;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.taskExecutor = taskExecutor;
        this.eventProducer = eventProducer;

        logger.info("Initialized RabbitMqConsumer. Target Queue: {}, Idle Timeout: {}ms",
                queueName, idleTimeoutMillis);
    }

    /**
     * Interrupts the primary consumption thread to facilitate a clean application exit.
     * <p>
     * This method is intended for use by JVM shutdown hooks or the internal idle-checker
     * to ensure network resources are released before the container terminates.
     * </p>
     */
    public void stopConsuming() {
        if (this.mainThread != null) {
            logger.info("Interrupting main consumer thread to initiate clean shutdown...");
            this.mainThread.interrupt();
        }
    }

    /**
     * Establishes a persistent connection to the broker and enters the primary consumption loop.
     * <p>
     * This method configures the channel QoS for fair task distribution and launches
     * the background idle-monitoring thread before blocking until the consumer is stopped.
     * </p>
     *
     * @throws Exception If broker connectivity fails or queue declarations are rejected.
     */
    public void startConsuming() throws Exception {
        this.mainThread = Thread.currentThread();

        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            // Configure Dead Letter Exchange for failed task redirect
            java.util.Map<String, Object> queueArgs = new java.util.HashMap<>();
            queueArgs.put("x-dead-letter-exchange", "dead_letter_exchange");

            channel.queueDeclare(queueName, true, false, false, queueArgs);
            channel.basicQos(1); // One task at a time per worker thread

            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                lastActivity.set(System.currentTimeMillis());
                handleMessage(channel, delivery);
            };

            channel.basicConsume(queueName, false, deliverCallback, (CancelCallback) consumerTag -> {});

            // Start the scale-to-zero monitor
            Thread idleChecker = getThread(lastActivity);
            idleChecker.start();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                logger.warn("Main consumer thread interrupted. Releasing resources...");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Processes individual message deliveries, manages error states, and signals completion.
     * <p>
     * This method implements the "Acknowledge after Execution" pattern to ensure
     * no data is lost if a worker crashes mid-task. It also handles the logic for
     * discarding Poison Pills and reporting class-level exceptions to the Manager.
     * </p>
     *
     * @param channel  The active AMQP channel.
     * @param delivery The delivery envelope containing the JSON task body.
     * @throws IOException If the broker communication for ACKs/NACKs is severed.
     */
    private void handleMessage(Channel channel, com.rabbitmq.client.Delivery delivery) throws IOException {
        String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        long deliveryCount = getDeliveryCount(delivery.getProperties());

        String jobId = "UNKNOWN";
        String taskId = "UNKNOWN";
        String phase = "UNKNOWN";

        // Pre-parse for logging context
        try {
            JsonNode jsonNode = objectMapper.readTree(messageBody);
            jobId = jsonNode.path("jobId").asText("UNKNOWN");
            taskId = jsonNode.path("taskId").asText("UNKNOWN");
            phase = jsonNode.path("taskType").asText("UNKNOWN");
        } catch (Exception e) {
            logger.warn("Metadata extraction failed; logging correlation will be degraded.");
        }

        try {
            // Tag all subsequent logs with the Job and Task identifiers
            MDC.put("job_id", jobId);
            MDC.put("task_id", taskId);

            // Safety check for infinite retry loops
            if (deliveryCount > MAX_RETRIES) {
                logger.error("Poison Pill Detected! Task exceeded MAX_RETRIES ({}).", MAX_RETRIES);
                eventProducer.sendCompletionSignal(jobId, taskId, "FAILED", "MAX_RETRIES_EXCEEDED");
                channel.basicNack(deliveryTag, false, false); // Send to DLX
                return;
            }

            try {
                taskExecutor.executeTask(messageBody);

                // Reduce tasks signal here; Map tasks signal within executor to include network identity
                if (!"MAP".equalsIgnoreCase(phase)) {
                    eventProducer.sendCompletionSignal(jobId, taskId, "COMPLETED", null);
                }

                channel.basicAck(deliveryTag, false);

            } catch (Throwable t) {
                logger.error("Transient task failure. Re-queuing and signaling FAILED state.", t);
                eventProducer.sendCompletionSignal(jobId, taskId, "FAILED", t.getClass().getSimpleName());
                channel.basicNack(deliveryTag, false, true); // Re-queue for another worker
            }
        } finally {
            MDC.clear();
        }
    }

    /**
     * Extracts the current delivery attempt count from message properties.
     *
     * @param properties The RabbitMQ message properties.
     * @return The delivery count, defaulting to 1 if the header is absent.
     */
    private long getDeliveryCount(AMQP.BasicProperties properties) {
        Map<String, Object> headers = properties.getHeaders();
        if (headers != null && headers.containsKey("x-delivery-count")) {
            return ((Number) headers.get("x-delivery-count")).longValue();
        }
        return 1;
    }

    /**
     * Creates a daemon background thread to monitor worker inactivity.
     * <p>
     * Uses an adaptive sleep interval based on the remaining time until the
     * threshold is reached, ensuring low CPU overhead.
     * </p>
     *
     * @param lastActivity Atomic timestamp reflecting the most recent message delivery.
     * @return A configured daemon {@link Thread} ready for execution.
     */
    @NotNull
    private Thread getThread(AtomicLong lastActivity) {
        Thread idleChecker = new Thread(() -> {
            try {
                while (true) {
                    long elapsed = System.currentTimeMillis() - lastActivity.get();
                    long remainingToWait = idleTimeoutMillis - elapsed;

                    if (remainingToWait <= 0) {
                        logger.info("Inactivity threshold reached ({}s). Terminating.", (idleTimeoutMillis / 1000));
                        stopConsuming();
                        break;
                    }
                    Thread.sleep(Math.max(1000, remainingToWait));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        idleChecker.setDaemon(true);
        return idleChecker;
    }
}