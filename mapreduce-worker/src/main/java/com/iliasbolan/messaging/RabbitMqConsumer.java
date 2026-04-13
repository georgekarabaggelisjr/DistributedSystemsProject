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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the connection to the Message Broker (RabbitMQ) and consumes task assignments.
 * <p>
 * This class serves as the primary lifecycle controller for the Worker node. It connects
 * to the specified RabbitMQ queue, consumes messages asynchronously, and now implements
 * an Enterprise 2-Queue Architecture:
 * 1. Consumes from the Work Queue.
 * 2. Publishes real-time status events to the Audit/Status Queue.
 * </p>
 */
public class RabbitMqConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final RabbitMqConnectionManager connectionManager;
    private final String queueName;
    private final int idleTimeoutMillis;
    private final TaskExecutor taskExecutor;

    // The designated queue for broadcasting progress to George's Master Node
    private final String statusQueueName = "job_events_queue";

    private Thread mainThread;

    public RabbitMqConsumer(RabbitMqConnectionManager connectionManager, String queueName, int idleTimeoutMillis, TaskExecutor taskExecutor) {
        this.connectionManager = connectionManager;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.taskExecutor = taskExecutor;

        logger.info("Initialized RabbitMqConsumer. Target Queue: {}, Status Queue: {}, Idle Timeout: {}ms",
                queueName, statusQueueName, idleTimeoutMillis);
    }

    public void stopConsuming() {
        if (this.mainThread != null) {
            logger.info("Interrupting main consumer thread to initiate clean shutdown...");
            this.mainThread.interrupt();
        }
    }

    public void startConsuming() throws Exception {
        this.mainThread = Thread.currentThread();

        try (Connection connection = connectionManager.createConnection();
             Channel channel = connection.createChannel()) {

            // 1. Declare both the Work Queue and the Status Queue
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueDeclare(statusQueueName, true, false, false, null);
            channel.basicQos(1);

            logger.info("Successfully connected to RabbitMQ. Waiting for messages on queue: '{}'.", queueName);

            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    lastActivity.set(System.currentTimeMillis());
                    String messageBody = new String(body, StandardCharsets.UTF_8);
                    long deliveryTag = envelope.getDeliveryTag();

                    // Safely extract identifying info for our status updates
                    String jobId = "UNKNOWN";
                    String taskId = "UNKNOWN";
                    try {
                        JsonNode jsonNode = objectMapper.readTree(messageBody);
                        jobId = jsonNode.path("jobId").asText("UNKNOWN");
                        taskId = jsonNode.path("taskId").asText("UNKNOWN");
                    } catch (Exception e) {
                        logger.warn("Could not parse JSON payload to extract Job/Task IDs for status reporting.", e);
                    }

                    try (MDC.MDCCloseable mdc = MDC.putCloseable("deliveryTag", String.valueOf(deliveryTag))) {
                        logger.info("Received Task Payload: {}", messageBody);

                        try {
                            // 1. Execute the actual Map/Reduce Work
                            taskExecutor.executeTask(messageBody);

                            // 2. Publish the "COMPLETED" Event to the Status Queue
                            String successEvent = String.format("{\"jobId\": \"%s\", \"taskId\": \"%s\", \"state\": \"COMPLETED\"}", jobId, taskId);
                            channel.basicPublish("", statusQueueName, null, successEvent.getBytes(StandardCharsets.UTF_8));

                            // 3. ACK the Work Queue
                            channel.basicAck(deliveryTag, false);
                            logger.info("Task successfully processed, event broadcasted, and ACK sent.");

                        } catch (Exception e) {
                            logger.error("Critical error processing task. Sending FAILED event and NACKing.", e);

                            // 2. Publish the "FAILED" Event to the Status Queue
                            String failedEvent = String.format("{\"jobId\": \"%s\", \"taskId\": \"%s\", \"state\": \"FAILED\", \"error\": \"%s\"}",
                                    jobId, taskId, e.getClass().getSimpleName());
                            channel.basicPublish("", statusQueueName, null, failedEvent.getBytes(StandardCharsets.UTF_8));

                            // 3. NACK the Work Queue
                            channel.basicNack(deliveryTag, false, true);
                        }
                    }
                }
            };

            channel.basicConsume(queueName, false, consumer);

            Thread idleChecker = getThread(lastActivity);
            idleChecker.start();

            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                logger.warn("Main consumer thread interrupted. Releasing RabbitMQ resources...", e);
                Thread.currentThread().interrupt();
            }
        }
    }

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
                        Thread.sleep(remainingToWait);
                    }
                }
            } catch (InterruptedException e) {
                logger.warn("Idle checker thread interrupted.", e);
                Thread.currentThread().interrupt();
            }
        });

        idleChecker.setDaemon(true);
        return idleChecker;
    }
}