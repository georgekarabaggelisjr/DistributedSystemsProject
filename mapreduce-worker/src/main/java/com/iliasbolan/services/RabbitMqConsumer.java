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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrates the lifecycle of task consumption from the RabbitMQ message broker.
 * <p>
 * This class serves as the primary event-driven controller for the Worker node, implementing
 * essential distributed systems patterns—such as Competing Consumers, Poison Pill protection,
 * and Graceful Shutdown—to ensure reliability in containerized Kubernetes environments.
 * </p>
 * <p>
 * <b>Operational Intelligence & Scaling:</b><br>
 * It utilizes Mapped Diagnostic Context (MDC) to inject <code>job_id</code> and <code>task_id</code>
 * into every log statement triggered during message processing. Furthermore, it implements an
 * <i>Execution-Aware Scale-to-Zero</i> daemon that actively monitors idle time without interrupting
 * long-running sandbox computations.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.2
 * @since 2026-04-25
 * @see com.iliasbolan.engine.TaskExecutor
 */
public class RabbitMqConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);

    /**
     * Reusable JSON mapper configured to ignore unknown properties, ensuring
     * backward compatibility with evolving Manager API schemas.
     */
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /**
     * Threshold for message redelivery before a task is classified as a 'Poison Pill'
     * and moved to the Dead Letter Exchange (DLX).
     */
    private static final int MAX_RETRIES = 3;

    private final RabbitMqConnectionManager connectionManager;
    private final String queueName;
    private final int idleTimeoutMillis;
    private final TaskExecutor taskExecutor;
    private final RabbitMqProducer eventProducer;
    private Thread mainThread;

    /**
     * Initializes the consumer with its required dependencies and operational parameters.
     *
     * @param connectionManager Factory for creating RabbitMQ connections and channels.
     * @param queueName          The specific AMQP queue to monitor for task intents.
     * @param idleTimeoutMillis  Time in milliseconds of inactivity before the consumer triggers a self-termination signal.
     * @param taskExecutor       The computational engine that will process the localized tasks.
     * @param eventProducer      The signaling component for broadcasting task status back to the Manager.
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
     * Signals the main consumer thread to interrupt its blocking loop,
     * facilitating a clean release of AMQP resources.
     */
    public void stopConsuming() {
        if (this.mainThread != null) {
            logger.info("Interrupting main consumer thread to initiate clean shutdown...");
            this.mainThread.interrupt();
        }
    }

    /**
     * Establishes the RabbitMQ session and begins the blocking consumption loop.
     * <p>
     * This method configures the channel with <code>basicQos(1)</code> to ensure
     * fair dispatching among multiple worker pods and declares the dead-letter
     * topology for fault tolerance.
     * </p>
     *
     * @throws Exception If the connection is refused or the channel fails to initialize.
     */
    public void startConsuming() throws Exception {
        this.mainThread = Thread.currentThread();

        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            // Define DLX arguments and align Idempotent parameters with Orchestrator optimizations
            java.util.Map<String, Object> queueArgs = new java.util.HashMap<>();
            queueArgs.put("x-dead-letter-exchange", "dead_letter_exchange");
            queueArgs.put("x-queue-mode", "lazy");     // Crucial: Must match Python publisher to avoid PRECONDITION_FAILED
            queueArgs.put("x-expires", 86400000);      // 24-hour autonomous garbage collection

            channel.queueDeclare(queueName, true, false, false, queueArgs);

            // Set Prefetch count to 1 to implement the Competing Consumers pattern efficiently
            channel.basicQos(1);

            // Thread-safe activity and execution state trackers for the idle watchdog
            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());
            AtomicBoolean isProcessing = new AtomicBoolean(false);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                isProcessing.set(true); // Signal the watchdog to pause the countdown
                try {
                    handleMessage(channel, delivery);
                } finally {
                    lastActivity.set(System.currentTimeMillis()); // Reset idle timer AFTER execution
                    isProcessing.set(false); // Resume watchdog monitoring
                }
            };

            // Start consuming with manual acknowledgments enabled
            channel.basicConsume(queueName, false, deliverCallback, (CancelCallback) consumerTag -> {});

            // Initialize the background watchdog to monitor for inactivity
            Thread idleChecker = getThread(lastActivity, isProcessing);
            idleChecker.start();

            try {
                // Block the main thread until shutdown is signaled via interruption
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                logger.warn("Main consumer thread interrupted. Releasing resources...");
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Processes an individual message delivery, handling metadata extraction,
     * task execution, and manual retry tracking for Classic Lazy Queues.
     *
     * @param channel  The active RabbitMQ channel for ACKing or NACKing.
     * @param delivery The encapsulated AMQP delivery containing the body and envelopes.
     * @throws IOException If a channel-level communication error occurs.
     */
    private void handleMessage(Channel channel, com.rabbitmq.client.Delivery delivery) throws IOException {
        byte[] rawBody = delivery.getBody();
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();

        // --- Extract custom retry count instead of native Quorum count ---
        long deliveryCount = getCustomDeliveryCount(delivery.getProperties());

        String jobId = "UNKNOWN";
        String taskId = "UNKNOWN";
        String jobToken = null;

        // PERFORMANCE OPTIMIZATION: Zero-Copy JSON Parsing
        // Bypasses intermediate String creation, allowing Jackson to parse the byte array directly.
        try {
            JsonNode jsonNode = objectMapper.readTree(rawBody);
            jobId = jsonNode.path("jobId").asText("UNKNOWN");
            taskId = jsonNode.path("taskId").asText("UNKNOWN");
            jobToken = jsonNode.path("jobToken").asText(null);
        } catch (Exception e) {
            logger.warn("Metadata extraction failed; logging correlation will be degraded.");
        }

        try {
            // Contextual Logging: Inject IDs into MDC for distributed tracing
            MDC.put("job_id", jobId);
            MDC.put("task_id", taskId);

            // Poison Pill Check: Prevent infinite retry loops of faulty tasks
            if (deliveryCount > MAX_RETRIES) {
                logger.error("Poison Pill Detected! Task exceeded MAX_RETRIES ({}). Routing to DLX.", MAX_RETRIES);
                eventProducer.sendErrorSignal(jobId, taskId, jobToken, "FAILED", "MAX_RETRIES_EXCEEDED");

                // NACK with requeue=false forces the lazy queue to push it to the Dead Letter Exchange
                channel.basicNack(deliveryTag, false, false);
                return;
            }

            try {
                // Decode string exactly once for the Executor sandbox
                String messageBody = new String(rawBody, StandardCharsets.UTF_8);

                // Delegate computation to the TaskExecutor (Sandbox)
                taskExecutor.executeTask(messageBody);

                // Explicit positive acknowledgment on success
                channel.basicAck(deliveryTag, false);

            } catch (Throwable t) {
                logger.error("Transient task failure. Republishing with incremented retry count.", t);

                try {
                    // Isolate the network call so a secondary failure doesn't hijack the retry logic
                    eventProducer.sendErrorSignal(jobId, taskId, jobToken, "FAILED", t.getClass().getSimpleName());
                } catch (Exception networkError) {
                    logger.error("Failed to broadcast error signal. Proceeding with local requeue.", networkError);
                } finally {
                    // --- Republish-with-Header Pattern ---
                    // 1. Create a mutable copy of the headers and increment the custom retry count
                    java.util.Map<String, Object> headers = delivery.getProperties().getHeaders();
                    if (headers == null) {
                        headers = new java.util.HashMap<>();
                    } else {
                        headers = new java.util.HashMap<>(headers); // Ensure mutability
                    }
                    headers.put("x-custom-retry-count", deliveryCount + 1);

                    AMQP.BasicProperties retryProps = new AMQP.BasicProperties.Builder()
                            .headers(headers)
                            .deliveryMode(2) // Ensure the retry message is persistent on disk
                            .contentType(delivery.getProperties().getContentType())
                            .build();

                    // 2. Publish the new message to the back of the queue
                    // Note: "" as exchange routes it directly to the queueName
                    channel.basicPublish("", this.queueName, retryProps, rawBody);

                    // 3. ACK the original failing message so it is successfully dequeued
                    channel.basicAck(deliveryTag, false);
                    // ------------------------------------------
                }
            }
        } finally {
            MDC.clear(); // Ensure thread-local context is purged for the next delivery
        }
    }

    /**
     * Extracts the custom delivery count from AMQP headers to support
     * Poison Pill detection on Classic Lazy Queues.
     *
     * @param properties The AMQP properties associated with the current delivery.
     * @return The number of times this message has been locally retried.
     */
    private long getCustomDeliveryCount(AMQP.BasicProperties properties) {
        java.util.Map<String, Object> headers = properties.getHeaders();
        if (headers != null && headers.containsKey("x-custom-retry-count")) {
            Object countStr = headers.get("x-custom-retry-count");
            if (countStr instanceof Number) {
                return ((Number) countStr).longValue();
            }
        }
        return 1; // Default to 1 for the initial attempt
    }

    /**
     * Creates a daemon thread that monitors inactivity and initiates a pod shutdown
     * once the idle threshold is crossed.
     * <p>
     * The thread is execution-aware; it pauses its countdown while the worker is actively
     * executing a task to prevent premature termination during long-running computations.
     * </p>
     *
     * @param lastActivity A thread-safe pointer to the last timestamp of message delivery.
     * @param isProcessing A thread-safe flag indicating if the worker is currently computing a task.
     * @return A pre-configured background thread for idle monitoring.
     */
    @NotNull
    private Thread getThread(AtomicLong lastActivity, AtomicBoolean isProcessing) {
        return new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // Execution-Aware Check: Pause countdown if actively working
                    if (isProcessing.get()) {
                        Thread.sleep(1000);
                        continue;
                    }

                    long elapsed = System.currentTimeMillis() - lastActivity.get();
                    long remainingToWait = idleTimeoutMillis - elapsed;

                    if (remainingToWait <= 0) {
                        logger.info("Inactivity threshold reached ({}s). Terminating pod.", (idleTimeoutMillis / 1000));
                        stopConsuming();
                        break;
                    }
                    Thread.sleep(Math.max(1000, remainingToWait));
                }
            } catch (InterruptedException e) {
                logger.info("Idle monitor interrupted. Shutting down.");
                Thread.currentThread().interrupt();
            }
        });
    }
}