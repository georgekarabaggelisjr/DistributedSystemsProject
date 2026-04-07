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
 * @version 1.5
 * @since 2026-03-30
 */
public class RabbitMqConsumer {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConsumer.class);

    private final String host;
    private final String username;
    private final String password;
    private final String queueName;
    private final int idleTimeoutMillis;
    private final TaskExecutor taskExecutor;

    private Thread mainThread;

    /**
     * Initializes the RabbitMQ Consumer configuration with secure credentials.
     *
     * @param host              The hostname of the RabbitMQ server.
     * @param username          The RabbitMQ authentication username.
     * @param password          The RabbitMQ authentication password.
     * @param queueName         The name of the queue to consume from.
     * @param idleTimeoutMillis The maximum time (in milliseconds) to wait for a new message before shutting down.
     * @param taskExecutor      The central orchestration engine for executing Map-Reduce tasks.
     */
    public RabbitMqConsumer(String host, String username, String password, String queueName, int idleTimeoutMillis, TaskExecutor taskExecutor) {
        this.host = host;
        this.username = username;
        this.password = password;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.taskExecutor = taskExecutor;

        // We purposefully DO NOT log the password here for security reasons!
        logger.info("Initialized RabbitMqConsumer. Host: {}, User: {}, Target Queue: {}, Idle Timeout: {}ms",
                host, username, queueName, idleTimeoutMillis);
    }

    public void stopConsuming() {
        if (this.mainThread != null) {
            logger.info("Interrupting main consumer thread to initiate clean shutdown...");
            this.mainThread.interrupt();
        }
    }

    public void startConsuming() throws Exception {
        this.mainThread = Thread.currentThread();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);

        // Dynamically apply the credentials injected by the Environment Variables
        factory.setUsername(username);
        factory.setPassword(password);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, true, false, false, null);
            channel.basicQos(1);

            logger.info("Successfully connected to RabbitMQ. Waiting for messages on queue: '{}'.", queueName);

            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    lastActivity.set(System.currentTimeMillis());
                    String messageBody = new String(body, StandardCharsets.UTF_8);
                    long deliveryTag = envelope.getDeliveryTag();

                    try (MDC.MDCCloseable mdc = MDC.putCloseable("deliveryTag", String.valueOf(deliveryTag))) {
                        logger.info("Received Task Payload: {}", messageBody);
                        try {
                            taskExecutor.executeTask(messageBody);
                            channel.basicAck(deliveryTag, false);
                            logger.info("Task successfully processed and acknowledged.");
                        } catch (Exception e) {
                            logger.error("Critical error processing task. Sending NACK to requeue message.", e);
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
                        System.exit(0);
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