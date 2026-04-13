package com.iliasbolan.messaging;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Manages the connection configuration and lifecycle for RabbitMQ.
 * <p>
 * By extracting the connection logic into this dedicated manager, we adhere to the
 * Single Responsibility Principle. Any class in the application that needs to communicate
 * with RabbitMQ can request a connection from this manager without needing to know
 * the underlying authentication details.
 * </p>
 */
public class RabbitMqConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqConnectionManager.class);
    private final ConnectionFactory factory;


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
     * @return A live RabbitMQ {@link Connection}
     * @throws IOException If a network error occurs
     * @throws TimeoutException If the connection times out
     */
    public Connection createConnection() throws IOException, TimeoutException {
        logger.debug("Establishing new TCP connection to RabbitMQ...");
        return factory.newConnection();
    }
}