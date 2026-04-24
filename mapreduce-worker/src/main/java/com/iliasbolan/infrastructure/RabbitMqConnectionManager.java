package com.iliasbolan.infrastructure;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Manages the connection configuration and lifecycle for the RabbitMQ message broker.
 *
 * <p><b>Design Principles:</b><br>
 * This class adheres to the <i>Single Responsibility Principle (SRP)</i> by isolating
 * the complexities of connection management and authentication logic. By centralizing
 * the {@link ConnectionFactory} configuration, the application ensures that all
 * RabbitMQ clients—whether producers or consumers—utilize a consistent and secure
 * authentication mechanism without requiring local knowledge of the infrastructure
 * credentials.</p>
 *
 * <p><b>Lifecycle Management:</b><br>
 * This manager provides the necessary abstractions for generating fresh, authenticated
 * TCP connections. It is the responsibility of the calling component to manage
 * the lifecycle of the returned {@link Connection} (e.g., ensuring proper
 * closure via try-with-resources blocks).</p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-03-30
 * @see com.rabbitmq.client.Connection
 * @see com.rabbitmq.client.ConnectionFactory
 */
public class RabbitMqConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConnectionManager.class);

    /** The underlying factory configured with broker authentication details. */
    private final ConnectionFactory factory;

    /**
     * Constructs a new {@code RabbitMqConnectionManager} with secure credentials.
     *
     * <p>Initializes the {@link ConnectionFactory} with the provided host and
     * authentication identity to enable subsequent connection requests.</p>
     *
     * @param host     The hostname or IP address of the RabbitMQ broker.
     * @param username The authenticated username for the broker service.
     * @param password The password associated with the provided username.
     */
    public RabbitMqConnectionManager(String host, String username, String password) {
        this.factory = new ConnectionFactory();
        this.factory.setHost(host);
        this.factory.setUsername(username);
        this.factory.setPassword(password);

        logger.info("RabbitMqConnectionManager initialized securely for host: {}", host);
    }

    /**
     * Generates a fresh, authenticated connection to the RabbitMQ broker.
     *
     * <p>This method initiates a new TCP connection and performs the necessary
     * AMQP-level handshake using the credentials established during construction.</p>
     *
     * @return A live, authenticated RabbitMQ {@link Connection} ready for channel creation.
     * @throws IOException      If a network-level error occurs during the connection attempt.
     * @throws TimeoutException If the broker fails to respond within the configured timeout period.
     */
    public Connection createConnection() throws IOException, TimeoutException {
        logger.debug("Establishing new TCP connection to RabbitMQ...");
        return factory.newConnection();
    }
}