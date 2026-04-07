package com.iliasbolan.messaging;

import com.iliasbolan.engine.TaskExecutor;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
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
 * to the specified RabbitMQ queue and consumes messages asynchronously (e.g., 64MB chunk assignments).
 * To guarantee At-Least-Once delivery and fault tolerance, it uses manual Acknowledgments (ACKs).
 * If the worker successfully processes the task and writes to MinIO, it sends an ACK.
 * If the worker crashes, the unacknowledged message is automatically re-queued.
 * </p>
 * <p>
 * <b>Graceful Termination:</b> Because Workers run as Kubernetes Jobs, they must not run forever.
 * This consumer implements an idle timeout. If no messages are received for a sustained period,
 * it assumes the job phase is complete and intentionally exits the JVM with status 0,
 * signaling completion to Kubernetes.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.3
 * @since 2026-03-30
 */
public class RabbitMqConsumer {

    // Instantiate the SLF4J Logger specific to this class
    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);

    private final String host;
    private final String queueName;
    private final int idleTimeoutMillis;
    private final TaskExecutor taskExecutor;

    /**
     * Initializes the RabbitMQ Consumer configuration.
     *
     * @param host              The hostname of the RabbitMQ server (e.g., "rabbitmq" or "localhost").
     * @param queueName         The name of the queue to consume from (e.g., "map_tasks_queue").
     * @param idleTimeoutMillis The maximum time (in milliseconds) to wait for a new message before shutting down.
     * @param taskExecutor      The central orchestration engine for executing Map-Reduce tasks.
     */
    public RabbitMqConsumer(String host, String queueName, int idleTimeoutMillis, TaskExecutor taskExecutor) {
        this.host = host;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.taskExecutor = taskExecutor;

        logger.info("Initialized RabbitMqConsumer. Host: {}, Target Queue: {}, Idle Timeout: {}ms",
                host, queueName, idleTimeoutMillis);
    }

    /**
     * Starts the event-driven consumption to consume messages from the queue.
     * <p>
     * This method sets up an asynchronous consumer using {@link Channel#basicConsume(String, boolean, Consumer)}.
     * It registers a callback that handles incoming messages, processes them, and sends manual ACKs.
     * If no messages are received for the idle timeout period, the worker terminates gracefully.
     * </p>
     *
     * @throws Exception If a critical connection error occurs.
     */
    public void startConsuming() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        // In a real environment, you'd also set username/password here
        // factory.setUsername("guest");
        // factory.setPassword("guest");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Ensure the queue exists before trying to consume from it.
            channel.queueDeclare(queueName, true, false, false, null);

            // Prefetch count: tells RabbitMQ not to give more than 1 message at a time to this worker
            channel.basicQos(1);

            logger.info("Successfully connected to RabbitMQ. Waiting for messages on queue: '{}'.", queueName);

            // Thread-safe wrapper for tracking the last time we got a message
            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    // Update the thread-safe atomic long immediately upon receiving a message
                    lastActivity.set(System.currentTimeMillis());

                    String messageBody = new String(body, StandardCharsets.UTF_8);
                    long deliveryTag = envelope.getDeliveryTag();

                    // Inject the deliveryTag into MDC so all subsequent logs in this thread include it
                    try (MDC.MDCCloseable mdc = MDC.putCloseable("deliveryTag", String.valueOf(deliveryTag))) {

                        logger.info("Received Task Payload: {}", messageBody);

                        try {
                            // Execute the actual engine logic
                            taskExecutor.executeTask(messageBody);

                            // If the work finishes without throwing an exception, send the ACK
                            channel.basicAck(deliveryTag, false);
                            logger.info("Task successfully processed and acknowledged.");

                        } catch (Exception e) {
                            logger.error("Critical error processing task. Sending NACK to requeue message.", e);
                            // NACK the message so RabbitMQ re-queues it for another worker
                            channel.basicNack(deliveryTag, false, true);
                        }
                    }
                }
            };

            channel.basicConsume(queueName, false, consumer);

            // Smart Idle Checker (No Busy-Waiting)
            Thread idleChecker = getThread(lastActivity);
            idleChecker.start();

            // To keep the main thread alive while the consumer runs in the background
            try {
                Thread.currentThread().join();
            } catch (InterruptedException e) {
                logger.warn("Main consumer thread interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Creates a daemon thread that monitors for idle timeout and terminates the application if no messages are received within the specified period.
     *
     * @param lastActivity An AtomicLong tracking the timestamp of the last message activity.
     * @return A daemon Thread configured to check for idle timeout.
     */
    @NotNull
    private Thread getThread(AtomicLong lastActivity) {
        Thread idleChecker = new Thread(() -> {
            try {
                while (true) {
                    // Calculate how much time has passed since the last message
                    long elapsed = System.currentTimeMillis() - lastActivity.get();

                    // Calculate exactly how much time is left until the timeout expires
                    long remainingToWait = idleTimeoutMillis - elapsed;

                    if (remainingToWait <= 0) {
                        // The timeout has been reached or exceeded
                        logger.info("No messages received for {} seconds. Gracefully terminating worker phase.", (idleTimeoutMillis / 1000));
                        System.exit(0);
                    } else {
                        // Sleep for the exact remaining time instead of polling every second
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