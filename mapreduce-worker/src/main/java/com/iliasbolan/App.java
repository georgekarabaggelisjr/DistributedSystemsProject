package com.iliasbolan;

import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.messaging.RabbitMqConnectionManager;
import com.iliasbolan.messaging.RabbitMqConsumer;
import com.iliasbolan.storage.MinioConnectionManager;
import com.iliasbolan.storage.S3ClientService;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * The main entry point for the Map-Reduce Worker application.
 * <p>
 * This class is designed to run in a containerized environment (Docker/Kubernetes).
 * It prioritizes reading configuration from Environment Variables, falling back to
 * safe "localhost" defaults for local development. It initializes the core services
 * and starts the event-driven RabbitMQ consumer loop.
 * </p>
 * <p>
 * <b>Graceful Shutdown:</b> This application registers a JVM shutdown hook to intercept
 * OS termination signals (e.g., Kubernetes SIGTERM). This ensures that the application
 * safely interrupts active tasks and closes network connections cleanly before exiting.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 1.6
 * @since 2026-03-30
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        logger.info("====== [ Map-Reduce Worker Initializing ] ======");

        // 1. Load Configurations
        String rabbitHost = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String rabbitUser = System.getenv().getOrDefault("RABBITMQ_USER", "guest");
        String rabbitPass = System.getenv().getOrDefault("RABBITMQ_PASS", "guest");
        String queueName = System.getenv().getOrDefault("RABBITMQ_QUEUE", "map_tasks_queue");
        int idleTimeout = Integer.parseInt(System.getenv().getOrDefault("IDLE_TIMEOUT_MILLIS", "60000"));

        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioUser = System.getenv().getOrDefault("MINIO_ROOT_USER", "minioadmin");
        String minioPass = System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "minioadmin");

        try {
            logger.info("Loaded configuration. RabbitMQ Host: {}, Target Queue: {}, MinIO Host: {}",
                    rabbitHost, queueName, minioEndpoint);

            // 2. Initialize the Infrastructure Connection Managers
            RabbitMqConnectionManager rabbitManager = new RabbitMqConnectionManager(rabbitHost, rabbitUser, rabbitPass);
            MinioConnectionManager minioManager = new MinioConnectionManager(minioEndpoint, minioUser, minioPass);

            // 3. Initialize the Core Services (Dependency Injection)
            S3ClientService s3ClientService = new S3ClientService(minioManager);
            TaskExecutor taskExecutor = new TaskExecutor(s3ClientService);
            RabbitMqConsumer consumer = new RabbitMqConsumer(rabbitManager, queueName, idleTimeout, taskExecutor);

            /* --------------------------------*/
            /* --- KUBERNETES HEALTH PROBE --- */
            /* --------------------------------*/
            // Start a lightweight, native HTTP server on port 8080 so K8s knows the pod is alive
            HttpServer healthServer = HttpServer.create(new InetSocketAddress(8080), 0);
            healthServer.createContext("/health", exchange -> {
                String response = "OK";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            healthServer.setExecutor(null);
            healthServer.start();
            logger.info("Health check server listening on port 8080 (/health)...");

            // --- GRACEFUL SHUTDOWN HOOK ---
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.warn(">>> OS Termination Signal (SIGTERM) received! <<<");
                logger.warn("Initiating graceful shutdown of Worker node to prevent data corruption...");

                // Stop the network services cleanly
                healthServer.stop(0);
                consumer.stopConsuming();

                try {
                    // Give active Map/Reduce tasks a brief moment to finish current execution
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}

                logger.info("Worker node shut down successfully. Goodbye!");
            }));

            // 4. Start the event-driven listening loop
            logger.info("Starting RabbitMQ consumer loop...");
            consumer.startConsuming();

        } catch (Exception e) {
            logger.error("Worker failed to start due to a critical error.", e);
            System.exit(1);
        }
    }
}