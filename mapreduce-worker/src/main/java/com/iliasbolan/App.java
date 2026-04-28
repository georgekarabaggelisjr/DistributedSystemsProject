package com.iliasbolan;

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
 * This class orchestrates the bootstrap sequence of a stateless compute worker, ensuring all
 * infrastructure dependencies are resolved before entering the task consumption loop.
 * Key responsibilities include:
 * </p>
 * <ul>
 * <li><b>Configuration Management:</b> Resolving environment-based parameters for cloud-native deployment.</li>
 * <li><b>Resource Wiring:</b> Initializing connection managers for RabbitMQ and S3-compatible storage (MinIO).</li>
 * <li><b>Computational Steering:</b> Wiring the execution engine for Map and Reduce phases.</li>
 * <li><b>Observability:</b> Exposing a lightweight HTTP health probe for Kubernetes liveness and readiness checks.</li>
 * <li><b>Lifecycle Management:</b> Implementing graceful shutdown hooks to handle pod eviction and signal termination.</li>
 * </ul>
 * * <h3>Architecture Context: ESS Integration</h3>
 * <p>
 * In alignment with the <b>Version 2.0</b> specification, this worker operates in a sidecar-dependent
 * mode where gRPC Shuffle Server responsibilities are delegated to the <b>External Shuffle Service (ESS)</b>
 * DaemonSet. This architecture ensures compute nodes remain entirely ephemeral and stateless.
 * </p>
 *
 * @author Ilias Bolanakis
 * @version 2.1
 * @since 2026-04-25
 */
public class App {

    /**
     * Standard SLF4J logger for bootstrap and lifecycle events.
     */
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    /**
     * Application execution entry point. Performs dependency injection and starts the
     * blocking event-driven consumer loop.
     *
     * @param args Command line arguments (Configuration is derived from Environment Variables).
     */
    public static void main(String[] args) {
        logger.info("====== [ Map-Reduce Ephemeral Compute Worker Initializing ] ======");

        // 1. Environment-Based Configuration
        // Control Plane (RabbitMQ) parameters for managing task distribution and signaling.
        String rabbitHost = System.getenv().getOrDefault("RABBITMQ_HOST", "localhost");
        String rabbitUser = System.getenv().getOrDefault("RABBITMQ_USER", "guest");
        String rabbitPass = System.getenv().getOrDefault("RABBITMQ_PASS", "guest");

        // DYNAMIC QUEUE RESOLUTION
        // Construct the isolated queue name using the injected Kubernetes environment variables.
        String jobId = System.getenv("JOB_ID");
        String phase = System.getenv().getOrDefault("PHASE", "map").toLowerCase();
        String taskQueue;

        if (jobId != null && !jobId.trim().isEmpty()) {
            taskQueue = phase + "_tasks_" + jobId;
        } else {
            // Fallback to legacy queue for local offline development
            taskQueue = System.getenv().getOrDefault("RABBITMQ_QUEUE", "map_tasks_queue");
            logger.warn("JOB_ID not found in environment. Falling back to default queue: {}", taskQueue);
        }
        // --------------------------------------------------

        String eventQueue = System.getenv().getOrDefault("EVENT_QUEUE", "job_events_queue");
        int idleTimeout = Integer.parseInt(System.getenv().getOrDefault("IDLE_TIMEOUT_MILLIS", "5000"));

        // Storage Plane (MinIO/S3) parameters for user-code and dataset retrieval.
        String minioEndpoint = System.getenv().getOrDefault("MINIO_ENDPOINT", "http://localhost:9000");
        String minioUser = System.getenv().getOrDefault("MINIO_ROOT_USER", "minioadmin");
        String minioPass = System.getenv().getOrDefault("MINIO_ROOT_PASSWORD", "minioadmin");

        // Data Plane (Local Disk for ESS Handoff)
        // This directory serves as the handoff point between the compute pod and the host ESS.
        String baseShuffleDir = System.getenv().getOrDefault("SHUFFLE_DIR", "/mnt/mapreduce-shuffle");

        // VITAL: Node IP extracted via K8s Downward API for ESS routing and Data Locality scheduling.
        String nodeIp = System.getenv().getOrDefault("NODE_IP", "127.0.0.1");

        try {
            // 2. Initialize Core Infrastructure Managers
            // These managers maintain stateful pools to RabbitMQ and S3 (MinIO).
            RabbitMqConnectionManager rabbitManager = new RabbitMqConnectionManager(rabbitHost, rabbitUser, rabbitPass);
            MinioConnectionManager minioManager = new MinioConnectionManager(minioEndpoint, minioUser, minioPass);
            S3ClientService s3ClientService = new S3ClientService(minioManager);

            // 3. Initialize Messaging Components
            // The Producer acts as the feedback loop signaling task transitions back to the Manager.
            RabbitMqProducer eventProducer = new RabbitMqProducer(rabbitManager, eventQueue);

            // 4. Initialize the Computational Engine
            // TaskExecutor encapsulates the logic for sandbox isolation and cross-node shuffle fetches.
            TaskExecutor taskExecutor = new TaskExecutor(s3ClientService, baseShuffleDir, eventProducer, nodeIp);

            // 5. Initialize the Consumer
            // RabbitMqConsumer implements the competing consumers pattern to drain the task queue.
            RabbitMqConsumer consumer = new RabbitMqConsumer(rabbitManager, taskQueue, idleTimeout, taskExecutor, eventProducer);

            /* --------------------------------------------------- */
            /* --- HTTP SERVER (CONTROL PLANE: K8S PROBES)     --- */
            /* --------------------------------------------------- */
            // Exposes port 8080 to satisfy Kubernetes Readiness and Liveness probes.
            HttpServer healthServer = HttpServer.create(new InetSocketAddress(8080), 0);
            healthServer.createContext("/health", exchange -> {
                String response = "OK";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });
            healthServer.setExecutor(null); // Use the default executor
            healthServer.start();
            logger.info("Health probe server active on port 8080 [/health]");

            // --- GRACEFUL SHUTDOWN LOGIC ---
            // Ensures that AMQP channels are closed and in-flight tasks are handled before pod termination.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.warn(">>> Shutdown signal received. Cleaning up worker resources... <<<");
                try {
                    healthServer.stop(0);
                    consumer.stopConsuming();
                    Thread.sleep(1000); // Wait for the network stack to flush pending ACKs
                } catch (Exception e) {
                    logger.error("Error during graceful shutdown: {}", e.getMessage());
                }
                logger.info("Worker shutdown complete.");
            }));

            // 6. Enter Main Execution Loop
            // Blocking call: the worker will stay in this loop until the queue is drained or interrupted.
            logger.info("Entering event-driven task loop for queue: {}", taskQueue);
            consumer.startConsuming();

            // 7. Cleanup and Exit
            logger.info("Task queue drained. Shutting down worker.");
            healthServer.stop(0);
            System.exit(0);

        } catch (Throwable e) {
            // Fatal errors during initialization trigger a non-zero exit code to alert K8s.
            logger.error("Fatal initialization error. Worker process terminating.", e);
            System.exit(1);
        }
    }
}