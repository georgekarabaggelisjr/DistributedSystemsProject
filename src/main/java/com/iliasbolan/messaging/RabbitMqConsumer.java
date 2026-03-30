package com.iliasbolan.messaging;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages the connection to the Message Broker (RabbitMQ) and consumes task assignments.
 * <p>
 * This class serves as the primary lifecycle controller for the Worker node. It connects 
 * to the specified RabbitMQ queue and consumes messages asynchronously (e.g., 64MB chunk assignments). 
 * To guarantee At-Least-Once delivery and fault tolerance, it uses manual Acknowledgments (ACKs)[cite: 262]. 
 * If the worker successfully processes the task and writes to MinIO, it sends an ACK. 
 * If the worker crashes, the unacknowledged message is automatically re-queued[cite: 262].
 * </p>
 * <p>
 * <b>Graceful Termination:</b> Because Workers run as Kubernetes Jobs, they must not run forever[cite: 112, 457]. 
 * This consumer implements an idle timeout. If no messages are received for a sustained period, 
 * it assumes the job phase is complete and intentionally exits the JVM with status 0, 
 * signaling completion to Kubernetes[cite: 457, 473].
 * </p>
 *
 * @author Ilias Bolankis
 * @version 1.1
 * @since 2026-03-30
 */
public class RabbitMqConsumer {

    private final String host;
    private final String queueName;
    private final int idleTimeoutMillis;

    /**
     * Initializes the RabbitMQ Consumer configuration.
     *
     * @param host              The hostname of the RabbitMQ server (e.g., "rabbitmq" or "localhost").
     * @param queueName         The name of the queue to consume from (e.g., "map_tasks_queue").
     * @param idleTimeoutMillis The maximum time (in milliseconds) to wait for a new message before shutting down.
     */
    public RabbitMqConsumer(String host, String queueName, int idleTimeoutMillis) {
        this.host = host;
        this.queueName = queueName;
        this.idleTimeoutMillis = idleTimeoutMillis;
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
            // (queueName, durable, exclusive, autoDelete, arguments)
            channel.queueDeclare(queueName, true, false, false, null);

            // Prefetch count: tells RabbitMQ not to give more than 1 message at a time to this worker
            channel.basicQos(1);

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            // Thread-safe wrapper for tracking the last time we got a message
            AtomicLong lastActivity = new AtomicLong(System.currentTimeMillis());

            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    // Update the thread-safe atomic long immediately upon receiving a message
                    lastActivity.set(System.currentTimeMillis());

                    String messageBody = new String(body, StandardCharsets.UTF_8);
                    long deliveryTag = envelope.getDeliveryTag();

                    System.out.println(" [x] Received Task: '" + messageBody + "'");

                    try {
                        // ==========================================================
                        // TODO: THIS IS WHERE YOU CALL YOUR ENGINE!
                        // 1. Parse the JSON message to get the S3 bucket/object info
                        // 2. Download the user code using S3ClientService
                        // 3. Run MapTaskProcessor or ReduceTaskProcessor
                        // 4. Run ShufflePartitioner (if Map task)
                        // ==========================================================

                        simulateWork(messageBody);

                        // If the work finishes without throwing an exception, send the ACK
                        channel.basicAck(deliveryTag, false);
                        System.out.println(" [v] Task Acknowledged.");

                    } catch (Exception e) {
                        System.err.println(" [!] Error processing task: " + e.getMessage());
                        // NACK the message so RabbitMQ re-queues it for another worker
                        channel.basicNack(deliveryTag, false, true);
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
                        System.out.println(" [-] No messages for " + (idleTimeoutMillis / 1000) + " seconds. Gracefully terminating.");
                        System.exit(0);
                    } else {
                        // Sleep for the exact remaining time instead of polling every second
                        Thread.sleep(remainingToWait);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        idleChecker.setDaemon(true);
        return idleChecker;
    }

    /**
     * A temporary helper method to simulate processing delay.
     */
    private void simulateWork(String task) throws InterruptedException {
        // Simulating the Fork/Join framework doing heavy lifting
        Thread.sleep(3000);
    }
}