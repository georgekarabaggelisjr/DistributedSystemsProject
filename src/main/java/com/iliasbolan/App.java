package com.iliasbolan;

import com.iliasbolan.messaging.RabbitMqConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for the Map-Reduce Worker application.
 * <p>
 * This class is designed to run in a containerized environment (Docker/Kubernetes).
 * It prioritizes reading configuration from Environment Variables, falling back to
 * safe "localhost" defaults for local development. It initializes the core services
 * and starts the event-driven RabbitMQ consumer loop.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.1
 * @since 2026-03-30
 */
public class App {

    // Instantiate the SLF4J Logger for this specific class
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        logger.info("====== [ Map-Reduce Worker Initializing ] ======");

        // 1. Load RabbitMQ Configuration
        String rabbitHost = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String queueName = System.getenv().getOrDefault("RABBITMQ_QUEUE", "map_tasks_queue");
        int idleTimeout = Integer.parseInt(System.getenv().getOrDefault("IDLE_TIMEOUT_MILLIS", "60000"));

        // 2. Load MinIO (S3) Configuration
        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioUser = System.getenv().getOrDefault("MINIO_ROOT_USER", "minioadmin");
        // String minioPass = System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "minioadmin");

        try {
            // Logback will automatically format these variables into the JSON output!
            logger.info("Loaded configuration. RabbitMQ Host: {}, Target Queue: {}, MinIO Host: {}",
                    rabbitHost, queueName, minioEndpoint);

            // 3. Initialize the Consumer
            RabbitMqConsumer consumer = new RabbitMqConsumer(rabbitHost, queueName, idleTimeout);

            // 4. Start the event-driven listening loop
            logger.info("Starting RabbitMQ consumer loop...");
            consumer.startConsuming();

        } catch (Exception e) {
            // Use logger.error and pass the exception to get a full, JSON-formatted stack trace
            logger.error("Worker failed to start due to a critical error.", e);
            System.exit(1);
        }
    }
}