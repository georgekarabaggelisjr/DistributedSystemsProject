package com.iliasbolan;

import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.messaging.RabbitMqConnectionManager;
import com.iliasbolan.messaging.RabbitMqConsumer;
import com.iliasbolan.storage.MinioConnectionManager;
import com.iliasbolan.storage.S3ClientService;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

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
 * <p>
 * <b>Peer-to-Peer (P2P) Shuffle Directory Service:</b><br>
 * In addition to responding to Kubernetes health probes, the embedded HTTP server now acts
 * as a localized data node. It exposes the <code>/shuffle</code> endpoint, allowing sibling
 * Reduce workers to fetch intermediate map partitions directly over the network. It supports
 * directory streaming, combining multiple Map-chunk files into a single network response.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.1
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
        int idleTimeout = Integer.parseInt(System.getenv().getOrDefault("IDLE_TIMEOUT_MILLIS", "5000"));

        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioUser = System.getenv().getOrDefault("MINIO_ROOT_USER", "minioadmin");
        String minioPass = System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "minioadmin");

        String baseShuffleDir = System.getenv().getOrDefault("SHUFFLE_DIR", "/tmp/shuffle-data");

        try {
            logger.info("Loaded configuration. RabbitMQ Host: {}, Target Queue: {}, MinIO Host: {}",
                    rabbitHost, queueName, minioEndpoint);

            RabbitMqConnectionManager rabbitManager = new RabbitMqConnectionManager(rabbitHost, rabbitUser, rabbitPass);
            MinioConnectionManager minioManager = new MinioConnectionManager(minioEndpoint, minioUser, minioPass);
            S3ClientService s3ClientService = new S3ClientService(minioManager);
            TaskExecutor taskExecutor = new TaskExecutor(s3ClientService, baseShuffleDir);
            RabbitMqConsumer consumer = new RabbitMqConsumer(rabbitManager, queueName, idleTimeout, taskExecutor);

            /* ------------------------------------------------ */
            /* --- EMBEDDED HTTP SERVER (HEALTH & P2P SHUFFLE) --- */
            /* ------------------------------------------------ */
            HttpServer embeddedServer = HttpServer.create(new InetSocketAddress(8080), 0);

            // Endpoint 1: Kubernetes Health Probe
            embeddedServer.createContext("/health", exchange -> {
                String response = "OK";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            // Endpoint 2: P2P Shuffle Directory Fetcher
            embeddedServer.createContext("/shuffle", exchange -> {
                if (!"GET".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                String path = exchange.getRequestURI().getPath();
                String relativePath = path.substring("/shuffle/".length());
                Path filePath = Paths.get(baseShuffleDir, relativePath).normalize();

                // Security mechanism: Prevent Directory Traversal attacks
                if (!filePath.startsWith(Paths.get(baseShuffleDir).normalize())) {
                    exchange.sendResponseHeaders(403, -1);
                    return;
                }

                if (Files.exists(filePath)) {
                    exchange.getResponseHeaders().add("Content-Type", "text/plain");

                    // Support streaming the entire partition directory
                    if (Files.isDirectory(filePath)) {
                        // 0 indicates a chunked transfer encoding (unknown total size)
                        exchange.sendResponseHeaders(200, 0);
                        try (OutputStream os = exchange.getResponseBody();
                             Stream<Path> paths = Files.list(filePath)) {

                            paths.filter(p -> p.toString().endsWith(".txt"))
                                    .forEach(p -> {
                                        try {
                                            Files.copy(p, os);
                                        } catch (IOException e) {
                                            logger.error("Failed to stream partition file: {}", p, e);
                                        }
                                    });
                        }
                    } else {
                        // Support fetching a single specific file
                        exchange.sendResponseHeaders(200, Files.size(filePath));
                        try (OutputStream os = exchange.getResponseBody()) {
                            Files.copy(filePath, os);
                        }
                    }
                } else {
                    String response = "Partition data not found.";
                    exchange.sendResponseHeaders(404, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                }
            });

            embeddedServer.setExecutor(null);
            embeddedServer.start();
            logger.info("Embedded server listening on port 8080. Contexts: [/health, /shuffle]");

            // --- GRACEFUL SHUTDOWN HOOK ---
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.warn(">>> OS Termination Signal (SIGTERM) received! <<<");
                logger.warn("Initiating graceful shutdown of Worker node to prevent data corruption...");

                embeddedServer.stop(0);
                consumer.stopConsuming();

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}

                logger.info("Worker node shut down successfully. Goodbye!");
            }));

            logger.info("Starting RabbitMQ consumer loop...");
            consumer.startConsuming();

            logger.info("Queue empty for configured duration. Shutting down.");
            embeddedServer.stop(0);
            logger.info("====== [ Worker Process Complete: Signaling Kubernetes Success ] ======");
            System.exit(0);

        } catch (Throwable e) {
            logger.error("Worker failed to start due to a critical error.", e);
            System.exit(1);
        }
    }
}