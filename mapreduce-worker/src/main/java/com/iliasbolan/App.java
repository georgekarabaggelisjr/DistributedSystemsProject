package com.iliasbolan;

import com.iliasbolan.engine.shuffle.ShuffleGrpcServer;
import com.iliasbolan.engine.TaskExecutor;
import com.iliasbolan.infrastructure.RabbitMqConnectionManager;
import com.iliasbolan.services.RabbitMqConsumer;
import com.iliasbolan.services.RabbitMqProducer;
import com.iliasbolan.infrastructure.MinioConnectionManager;
import com.iliasbolan.services.S3ClientService;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * The primary entry point for the distributed Map-Reduce Worker node.
 * <p>
 * This class handles the bootstrap sequence of the worker, including:
 * <ul>
 * <li>Parsing environment-based configuration for cloud-native deployment.</li>
 * <li>Initializing connection managers for RabbitMQ and S3-compatible storage (MinIO).</li>
 * <li>Spawning the gRPC Shuffle Server for peer-to-peer data streaming.</li>
 * <li>Exposing a lightweight HTTP health probe for Kubernetes orchestration.</li>
 * <li>Managing a graceful shutdown hook to prevent data loss during pod termination.</li>
 * </ul>
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.0
 * @since 2026-03-30
 */
public class App {

    private static final Logger logger = LoggerFactory.getLogger(App.class);

    /**
     * Application execution starts here.
     *
     * @param args Command line arguments (unused; configuration is driven by environment variables).
     */
    public static void main(String[] args) {
        logger.info("====== [ Map-Reduce Worker Initializing ] ======");

        // 1. Environment-Based Configuration
        // Control Plane (RabbitMQ)
        String rabbitHost = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String rabbitUser = System.getenv().getOrDefault("RABBITMQ_USER", "guest");
        String rabbitPass = System.getenv().getOrDefault("RABBITMQ_PASS", "guest");
        String taskQueue = System.getenv().getOrDefault("RABBITMQ_QUEUE", "map_tasks_queue");
        String eventQueue = System.getenv().getOrDefault("EVENT_QUEUE", "job_events_queue");
        int idleTimeout = Integer.parseInt(System.getenv().getOrDefault("IDLE_TIMEOUT_MILLIS", "5000"));

        // Storage Plane (MinIO/S3)
        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioUser = System.getenv().getOrDefault("MINIO_ROOT_USER", "minioadmin");
        String minioPass = System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "minioadmin");

        // Data Plane (gRPC P2P Shuffle & Network Identity)
        String baseShuffleDir = System.getenv().getOrDefault("SHUFFLE_DIR", "/tmp/shuffle-data");
        int grpcPort = Integer.parseInt(System.getenv().getOrDefault("SHUFFLE_GRPC_PORT", "50051"));

        // VITAL: Read the internal Pod IP provided by the Kubernetes Downward API.
        // This IP is used to build the callback address for the P2P Shuffle phase.
        String podIp = System.getenv().getOrDefault("POD_IP", "127.0.0.1");

        try {
            logger.info("Configuration Loaded. Pod IP: {}, gRPC Port: {}, Storage: {}",
                    podIp, grpcPort, minioEndpoint);

            // 2. Initialize Core Infrastructure Managers
            RabbitMqConnectionManager rabbitManager = new RabbitMqConnectionManager(rabbitHost, rabbitUser, rabbitPass);
            MinioConnectionManager minioManager = new MinioConnectionManager(minioEndpoint, minioUser, minioPass);
            S3ClientService s3ClientService = new S3ClientService(minioManager);

            // 3. Initialize Messaging Components
            // The Producer signals the Manager when tasks transition to 'COMPLETED'.
            RabbitMqProducer eventProducer = new RabbitMqProducer(rabbitManager, eventQueue);

            // 4. Initialize the Computational Engine
            // TaskExecutor now receives the podIp to report its exact location for shuffle data.
            TaskExecutor taskExecutor = new TaskExecutor(s3ClientService, baseShuffleDir, eventProducer, podIp);

            // 5. Initialize the Consumer
            // Manages the RabbitMQ message loop and routes tasks to the executor.
            RabbitMqConsumer consumer = new RabbitMqConsumer(rabbitManager, taskQueue, idleTimeout, taskExecutor, eventProducer);

            /* --------------------------------------------------- */
            /* --- gRPC SERVER (DATA PLANE: P2P SHUFFLE)       --- */
            /* --------------------------------------------------- */
            ShuffleGrpcServer grpcServer = new ShuffleGrpcServer(grpcPort, baseShuffleDir);
            grpcServer.start();

            /* --------------------------------------------------- */
            /* --- HTTP SERVER (CONTROL PLANE: K8S PROBES)     --- */
            /* --------------------------------------------------- */
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
            logger.info("Health probe server active on port 8080 [/health]");

            // --- GRACEFUL SHUTDOWN LOGIC ---
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.warn(">>> Shutdown signal received. Cleaning up worker resources... <<<");
                try {
                    grpcServer.stop();
                    healthServer.stop(0);
                    consumer.stopConsuming();
                    Thread.sleep(1000); // Allow remaining network packets to drain
                } catch (Exception e) {
                    logger.error("Error during graceful shutdown: {}", e.getMessage());
                }
                logger.info("Worker shutdown complete.");
            }));

            // 6. Enter Main Execution Loop
            logger.info("Entering event-driven task loop for queue: {}", taskQueue);
            consumer.startConsuming();

            // 7. Cleanup and Exit
            logger.info("Task queue drained. Shutting down worker.");
            grpcServer.stop();
            healthServer.stop(0);
            System.exit(0);

        } catch (Throwable e) {
            logger.error("Fatal initialization error. Worker process terminating.", e);
            System.exit(1);
        }
    }
}